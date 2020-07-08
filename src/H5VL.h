#ifndef H5VL_H__
#define H5VL_H__
#include "stdio.h"
#include "stdlib.h"
#include "hdf5.h"
#define MAX_NUM_CACHE_FILE 1000

enum local_storage_type {SSD, BURST_BUFFER, MEMORY};
enum cache_purpose {READ, WRITE};
enum cache_duration {PERMANENT, TEMPORAL};
enum claim_type {SOFT, HARD};
/* This define the storage to use. 
 */
typedef struct _LocalStorage {
  local_storage_type type; 
  char *path;
  hsize_t total_space;
  hsize_t avail_space;
} LocalStorage; 

/* This define the cache 
 */
typedef struct _LocalStorageCache {
  LocalStorage *storage; // pointing back to the storage where the cache is located.
  cache_purpose purpose;
  cache_duration duration;
  hsize_t total_space;
  hsize_t avail_space;
  hsize_t total_space_per_rank;
  hsize_t avail_space_per_rank; 
} LocalStorageCache;


herr H5LSset(LocalStorage *LS, local_storage_type type, hsize_t avail_space);
void H5LSclaim_space(LocalStorage *LS, hsize_t size, char *folder, claim_type type);
#endif
