#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "mapreduce.h"
#include <stdbool.h>

// key value pairs returned from mapper function
struct kv {
  char *key;
  char *value;
  bool counted; // boolean for get_next_combiner
};

// key value pairs of each partition
struct pkv {
  int numkvs; // current number of key value pairs
  int numAlloc; // number of kvs currently allocated
  int pNum; // partition number
  struct kv* kvs; 
  int index; // use in get_next_reducer
};

 // unique key along with last index found in kv, used in get_next_combiner
struct keyCount {
  char* key;
  int index;
};

struct mapThreadInfo {
  int index;
  pthread_t threadID; // pthread_t associated with this struct
  int numkvs; // current number of tokens (increment in MR_EmitToCombiner)
  int uniqueTokens;  // number of unique keys
  int numAlloc; // number of tokens allocated for
  struct kv* kvs; // key value pairs in the file
};

pthread_mutex_t lock; // main lock
pthread_mutex_t fileLock; // locking when using a file
pthread_mutex_t rwLock; // locking when using reducer_wrapper
pthread_mutex_t cwLock; // locking when using sending to combiner
pthread_mutex_t gncLock; // locking when using get_next_combiner
pthread_mutex_t gnrLock; // locking when using a get_next_reducer

// global varables assigned in MR_Run
Mapper m = NULL;
Reducer r = NULL;
Partitioner p = NULL;
Combiner c = NULL;
int numPartitions;
int numFiles; // total number of files
int filesProcessed = 0;
int numPartitions;
int numMappers = 0;
int pkvsCreated = 0;
char **files;  // array of file names
struct mapThreadInfo **mappers; // array of map structs
struct pkv **pkvs; // pkv struct for each reducer

// Wrapper functions for checking return values of pthread functions
void Pthread_create(pthread_t *thread,
                    const pthread_attr_t *attr,
                    void *(*start_routine)(void *),
                    void *arg);
void Pthread_mutex_lock(pthread_mutex_t *mutex);
void Pthread_mutex_unlock(pthread_mutex_t *mutex);
void Pthread_cond_wait(pthread_cond_t *cond, pthread_mutex_t *mutex);
void Pthread_cond_signal(pthread_cond_t *cond);
void Pthread_join(pthread_t thread, void **retval);
char* get_next_combiner(char *key);
char* get_next_reducer(char *key, int partition);
char* get_state(char *key, int partition);
char** getUniqueKeys(struct mapThreadInfo *m, struct kv* kvs);
void MR_EmitToCombiner(char *key, char *value);
void MR_EmitToReducer(char *key, char *value);
void MR_EmitReducerState(char* key, char* state, int partition_number);
void *mapper_wrapper(void *mapper_args);
void *reducer_wrapper(void *reducer_args);
int compare(const void* kv1, const void* kv2);
void pkvsInit();
void kvsInit();

char* lastKey = NULL; // last key that get_next_combiner has been called on
int lastIndex = 0; // last index key was found at in sored array

/*
Return value member of key value pair if another instance of key is found,
or return NULL if all have been counted
Called by the combiner function
*/
char *get_next_combiner(char *key){

  Pthread_mutex_lock(&gncLock);

  pthread_t id = pthread_self();
  struct mapThreadInfo* currThread = NULL;

  // find current thread
  for(int i = 0; i < numFiles; i++){
    if(pthread_equal(mappers[i]->threadID, id)){
      currThread = mappers[i];
      break;
    }
  }

  // make sure a matching thread was found
  if(!currThread){
    printf("currThread NULL\n");
    exit(0);
  }

  char* val = NULL;
  int index = 0;

  if(lastKey == NULL){
    lastKey = currThread->kvs[0].key;
  }

  // see if searching for last key for efficiency
  if(strcmp(lastKey, key) == 0){
    index = lastIndex;
  }
  // if different key, find first spot
  else{
    while(strcmp(currThread->kvs[index].key, key) != 0){
      // printf("finding first match, index = %d\n", index);
      index++;
    }
  }

  lastKey = key;

  // find first uncounted if available
  int found = 0;
  while(strcmp(currThread->kvs[index].key, key) == 0){
    if(!currThread->kvs[index].counted){
      found = 1;
      break;
    }
    index++;

    // no uncounted match
    if(index >= currThread->numkvs){
      break;
    }
  }

  // // uncounted match found
  if(found){
    lastIndex = index;
    val = currThread->kvs[index].value;
    currThread->kvs[index].counted = 1;
  }

  Pthread_mutex_unlock(&gncLock);

  return val;
}

/*
Return value member of key value pair if another instance of key is found,
or return NULL if all have been counted
Called by the reducer function
*/
char *get_next_reducer(char *key, int partition){

  Pthread_mutex_lock(&gnrLock);

  struct pkv *partStruct = pkvs[partition];
  int numkvs = partStruct->numkvs;
  char *val = NULL;

  // search for repeats of key in sorted array
  if(partStruct->index){

    // all checked
    if(partStruct->index > numkvs-1){
      partStruct->index = 0;
    }

    // repeat found
    else if(strcmp(partStruct->kvs[partStruct->index].key, key) == 0){
      val = partStruct->kvs[partStruct->index].value;
      partStruct->index++;
    }
    else{ // no more of same key, reset index to 0 to restart search
      partStruct->index = 0;
    }


  }

  // new key to search for
  else{
    for(int i = 0; i < numkvs; i++){
      if(strcmp(key, partStruct->kvs[i].key) == 0){
        val = partStruct->kvs[i].value;
        partStruct->index = i+1;
        break; // break if match found
      }
    }

  }

  Pthread_mutex_unlock(&gnrLock);

  return val;
}

/*
send each provided file to the mapper function
followed by the combiner function if one is provided
*/
void *mapper_wrapper(void *mapper_args){

  while(filesProcessed < numFiles){
    char* curFile;

    /* make sure each file only accessed once */
    Pthread_mutex_lock(&fileLock);

    struct mapThreadInfo *mapper = mapper_args;
    curFile = files[filesProcessed];
    filesProcessed++;

    Pthread_mutex_unlock(&fileLock);

    m(curFile);

    // run combiner if provided one
    if(c){

      pthread_mutex_lock(&cwLock);

      qsort(&mapper->kvs[0], mapper->numkvs,  sizeof(struct kv), compare);

      char* lastKey;
      for(int i = 0; i < mapper->numkvs; i++){
        if(i == 0){
          lastKey = mapper->kvs[i].key;
          c(mapper->kvs[i].key, get_next_combiner);
        }

        else{

          // don't call reducer if this key has been sent
          if(strcmp(lastKey, mapper->kvs[i].key) == 0){
            continue;
          }
          else{
            lastKey = mapper->kvs[i].key;

            c(mapper->kvs[i].key, get_next_combiner);
          }

        }

      }

      pthread_mutex_unlock(&cwLock);
    }

  }

  return mapper_args;
}

void *reducer_wrapper(void *reducer_args){

  char *lastKey;
  struct pkv *reducerParams = reducer_args;

  // call reducer for each unique key/value pair
  // reducerParams->kvs is sorted
  for(int i = 0; i < reducerParams->numkvs; i++){
    if(i == 0){
      lastKey = reducerParams->kvs[i].key;
      r(reducerParams->kvs[i].key, NULL, get_next_reducer, reducerParams->pNum);
    }

    else{
      // don't call reducer if this key has been sent
      if(strcmp(lastKey, reducerParams->kvs[i].key) == 0){
        continue;
      }
      else{
        lastKey = reducerParams->kvs[i].key;
        r(reducerParams->kvs[i].key, NULL, get_next_reducer, reducerParams->pNum);
      }
    }

  }

  return reducer_args;
}

// add keys and values to mapThreadInfo struct
void MR_EmitToCombiner(char *key, char *value){
  Pthread_mutex_lock(&lock);

  pthread_t id = pthread_self();

  // one combiner for each mapper
  for(int i = 0; i < numMappers; i++){
    if(pthread_equal(mappers[i]->threadID, id)){ // find correct mapper thread

      int n = mappers[i]->numkvs; // used for simplicity/less typing

      // store key and value
      mappers[i]->kvs[n].key = malloc(strlen(key) + 1);
      strcpy(mappers[i]->kvs[n].key, key);
      mappers[i]->kvs[n].value = malloc(strlen(value) + 1);
      strcpy(mappers[i]->kvs[n].value, value);
      mappers[i]->kvs[n].counted = 0;
      mappers[i]->numkvs++;

      // check there is enough space for new tokens
      if(mappers[i]->numkvs == mappers[i]->numAlloc){
        mappers[i]->numAlloc *= 2; // double space
        mappers[i]->kvs = (struct kv*) realloc(mappers[i]->kvs, (sizeof(struct kv)) * (mappers[i]->numAlloc));
        if(!mappers[i]->kvs){
          printf("mappers[i]->kvs is NULL\n");
          exit(0);
        }
      }

      break; // id match found, move on
    }
  }

  Pthread_mutex_unlock(&lock);

}


void MR_EmitToReducer(char *key, char *value){
  Pthread_mutex_lock(&lock);

  unsigned long partNumber = p(key, numPartitions);

    // create kv pointer array for each partition
    if(!pkvsCreated){
      pkvsInit();
    }

    // assign key and value to kv struct of correct partition

    struct pkv* currPart = pkvs[partNumber];
    int n = currPart->numkvs;

    currPart->kvs[n].key = malloc(strlen(key) + 1);
    currPart->kvs[n].value = malloc(strlen(value) + 1);
    strcpy(currPart->kvs[n].key, key);
    strcpy(currPart->kvs[n].value, value);
    currPart->numkvs++;

    // check for reallocation
    if(currPart->numAlloc == currPart->numkvs){
      currPart->numAlloc *= 2;
      currPart->kvs = realloc(currPart->kvs, sizeof(struct kv) * currPart->numAlloc);
      if(!currPart->kvs){
        printf("currPart->kvs is NULL\n");
        exit(0);
      }
    }

  Pthread_mutex_unlock(&lock);

}

void MR_Run(int argc, char *argv[],
        Mapper map, int num_mappers,
        Reducer reduce, int num_reducers,
        Combiner combine,
        Partitioner partition){

  pthread_t map_threads[num_mappers];
  pthread_t reducer_threads[num_reducers];
  m = map;
  r = reduce;
  p = partition; // if still NULL, use MR_DefaultHashPartition
  c = combine;
  numFiles = argc - 1; // - 1 for binary file included in argc
  numPartitions = num_reducers;
  mappers = malloc(sizeof(struct mapThreadInfo) * num_mappers);

  int rc = pthread_mutex_init(&lock, NULL);
  assert(rc == 0); // check lock success

  rc = pthread_mutex_init(&fileLock, NULL);
  assert(rc == 0); // check lock success

  rc = pthread_mutex_init(&rwLock, NULL);
  assert(rc == 0); // check lock success

  rc = pthread_mutex_init(&cwLock, NULL);
  assert(rc == 0); // check lock success

  rc = pthread_mutex_init(&gnrLock, NULL);
  assert(rc == 0); // check lock success

  rc = pthread_mutex_init(&gncLock, NULL);
  assert(rc == 0); // check lock success

  numMappers = num_mappers;

  // initialize files
  files = malloc(sizeof(char*) * numFiles);
  for(int i = 0; i < numFiles; i++){
    files[i] = malloc(strlen(argv[i+1])+1); // first files argv[1]
    strcpy(files[i], argv[i+1]);
  }

  //initialize and create mapper threads
  for(int z = 0; z < num_mappers; z++){
    mappers[z] = malloc(sizeof(struct mapThreadInfo));
    mappers[z]->numkvs = 0;
    mappers[z]->index = 0;
    Pthread_create(&map_threads[z], NULL, mapper_wrapper, (void*)mappers[z]);
    mappers[z]->threadID = map_threads[z];

  }

  kvsInit();

  // wait for mapper threads to exit
  for (int i = 0; i < num_mappers; i++){
      Pthread_join(map_threads[i], NULL);
  }

  // sort the kv array for each reducer thread
  for(int i = 0; i < num_reducers; i++){
    qsort(&pkvs[i]->kvs[0], pkvs[i]->numkvs, sizeof(struct kv), compare);
  }

  // Launch reduce threads to process intermediate data
  for(int i = 0; i < num_reducers; i++){
    Pthread_create(&reducer_threads[i], NULL, reducer_wrapper, (void*)pkvs[i]);
  }

  // wait for reducer threads to exit
  for(int i = 0; i < num_reducers; i++){
    Pthread_join(reducer_threads[i], NULL);
  }

  // free all memory used
  for(int i = 0; i < num_mappers; i++){
    for(int j = 0; j < mappers[i]->numkvs; j++){
      free(mappers[i]->kvs[j].key);
      free(mappers[i]->kvs[j].value);
    }

    free(mappers[i]->kvs);
    free(mappers[i]);
  }

  free(mappers);

  for(int i = 0; i < numFiles; i++){
    free(files[i]);
  }
  free(files);

  for(int i = 0; i < numPartitions; i++){
    for(int j = 0; j < pkvs[i]->numkvs; j++){
      free(pkvs[i]->kvs[j].key);
      free(pkvs[i]->kvs[j].value);
    }
    free(pkvs[i]->kvs);
    free(pkvs[i]);
  }
  free(pkvs);
  free(lastKey);
}

// sort by key and value
int compare(const void* kv1, const void* kv2){
  struct kv* first = (struct kv*) kv1;
  struct kv* second = (struct kv*) kv2;
  if(strcmp(first->key, second->key) == 0){
    return strcmp(first->value, second->value);
  }
  return strcmp(first->key, second->key);
}

void kvsInit(){
  for(int i = 0; i < numMappers; i++){
    mappers[i]->kvs = malloc(sizeof(struct kv) * 10); // 10 kvs to start
    mappers[i]->numAlloc = 10;
  }
}

// Initialize partition structs
// Done in either MR_EmitToCombiner or MR_EmitToReducer if no combiner used
void pkvsInit(){
  pkvs = malloc(sizeof(struct pkv) * numPartitions);
  for(int i = 0; i < numPartitions; i++){
    pkvs[i] = malloc(sizeof(struct pkv));
    pkvs[i]->kvs = malloc(sizeof(struct kv) * 10); // 10 kvs to start in each
    pkvs[i]->pNum = i;
    pkvs[i]->numkvs = 0;
    pkvs[i]->numAlloc = 10;
    pkvs[i]->index = 0;
  }
  pkvsCreated = 1;
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

// Wrapper function to make sure create causes no unexpected errors
void Pthread_create(pthread_t *thread,
                    const pthread_attr_t *attr,
                    void *(*start_routine)(void *),
                    void *arg){
  int rc = pthread_create(thread, attr, start_routine, arg);
  assert(rc == 0);
}

// Wrapper function to make sure lock causes no unexpected errors
void Pthread_mutex_lock(pthread_mutex_t *mutex) {
  int rc = pthread_mutex_lock(mutex);
  assert(rc == 0);
}

// Wrapper function to make sure unlock causes no unexpected errors
void Pthread_mutex_unlock(pthread_mutex_t *mutex) {
  int rc = pthread_mutex_unlock(mutex);
  assert(rc == 0);
}

// Wrapper function to make sure wait causes no unexpected errors
void Pthread_cond_wait(pthread_cond_t *cond, pthread_mutex_t *mutex) {
  int rc = pthread_cond_wait(cond, mutex);
  assert(rc == 0);
}

// Wrapper function to make sure signal causes no unexpected errors
void Pthread_cond_signal(pthread_cond_t *cond) {
  int rc = pthread_cond_signal(cond);
  assert(rc == 0);
}

// Wrapper function to make sure join causes no unexpected errors
void Pthread_join(pthread_t thread, void **retval){
  int rc = pthread_join(thread, retval);
  assert(rc == 0);
}
