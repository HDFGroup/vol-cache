#ifndef H5LS_H__
#define H5LS_H__
#include "stdio.h"
#include "stdlib.h"
#include "hdf5.h"
#include "time.h"
#define MAX_NUM_CACHE_FILE 1000
#define MAX_NUM_CACHE_ACCESS 1000
// various enum to define 
enum cache_storage {SSD, BURST_BUFFER, MEMORY};
enum cache_purpose {READ, WRITE, RDWR};
enum cache_duration {PERMANENT, TEMPORAL};
enum cache_claim {SOFT, HARD};
enum cache_replacement_policy {FIFO, LIFO, LRU, LFU};
typedef enum cache_storage cache_storage_t; 
typedef enum cache_purpose cache_purpose_t; 
typedef enum cache_duration cache_duration_t; 
typedef enum cache_claim cache_claim_t; 
typedef enum cache_replacement_policy cache_replacement_policy_t;

/* 
   This define the cache 
 */
typedef struct _AcessHistory {
  time_t time_stamp[MAX_NUM_CACHE_ACCESS]; 
  int count; 
} AccessHistory; 

typedef struct _LocalStorageCache {
    cache_purpose_t purpose;
    cache_duration_t duration;
    bool growable; 
    hsize_t mspace_total;
    hsize_t mspace_left;
    hsize_t mspace_per_rank_total;
    hsize_t mspace_per_rank_left; 
    hid_t fd; // the associate file
    char path[255];
    AccessHistory access_history; 
} LocalStorageCache;



/* This define the storage to use. 
 */
typedef struct _CacheList {
  LocalStorageCache *cache;
  void *target; // the target file/dataset for the cache
  struct _CacheList *next;
} CacheList;

typedef struct _LocalStorage {
  cache_storage_t storage;
  char path[255];
  hsize_t mspace_total;
  hsize_t mspace_left;
  CacheList *cache_list; 
  int num_cache;
  bool io_node;  // select I/O node for I/O
  cache_replacement_policy_t replacement_policy;
} LocalStorage; 

#ifdef __cplusplus
extern "C" {
#endif
  herr_t H5LSset(LocalStorage *LS,  cache_storage_t type, char *path, hsize_t avail_space);
  herr_t H5LSclaim_space(LocalStorage *LS, hsize_t size, cache_claim_t type, cache_replacement_policy_t crp);
  herr_t H5LSremove_cache_all(LocalStorage *LS);
  herr_t H5LSregister_cache(LocalStorage *LS, LocalStorageCache *cache, void *target);
  herr_t H5LSremove_cache(LocalStorage *LS, LocalStorageCache *cache);
  herr_t H5LSrecord_cache_access(LocalStorageCache *cache);
#ifdef __cplusplus
}
#endif
#endif
