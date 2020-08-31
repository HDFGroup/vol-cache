#include "H5LS.h"
#include <sys/types.h> 
#include <sys/stat.h> 
#include <unistd.h> 
#include <stdio.h> 
#include <stdlib.h> 
#include <string.h>
#include <dirent.h>
#include "H5VLpassthru_ext.h"


#include "debug.h"
#ifndef FAIL
#define FAIL -1
#endif
#ifndef SUCCEED
#define SUCCEED 0
#endif


/* H5LSset set the global local storage property
          LS - the local storage struct 
     storage - the type of storage [SSD, BURST_BUFFER, MEMORY]
        path - the path to the local storage
mspace_total - the capacity of the local storage in Bytes. 
 */
herr_t H5LSset(LocalStorage *LS, cache_storage_t storage, char *path, hsize_t mspace_total)
{
    LS->storage = storage;
    LS->mspace_total = mspace_total;
    LS->mspace_left = mspace_total; 
    LS->num_cache = 0; 
    struct stat sb;
    strcpy(LS->path, path);//check existence of the space
    if (storage == MEMORY || ( stat(path, &sb) == 0 && S_ISDIR(sb.st_mode))) {
      return 0; 
    } else {
#ifdef __APPLE__
      fprintf(__stderrp, "%s does not exist\n", path); 
#else
      fprintf(stderr, "%s does not exist\n", path); 
#endif
      exit(EXIT_FAILURE); 
    }
}



bool H5LScompare_cache(LocalStorageCache *a, LocalStorageCache *b, cache_replacement_policy_t replacement_policy) {
  /// if true, a should be selected, otherwise b. 
  bool agb = false; 
  double fa, fb; 
  switch (replacement_policy) { 
  case (LRU):
    agb = (a->access_history.time_stamp[a->access_history.count] 
	   < b->access_history.time_stamp[b->access_history.count]); 
    break; 
  case (FIFO):
    agb = (a->access_history.time_stamp[0] < b->access_history.time_stamp[0]); 
    break; 
  case (LFU):
    fa = (a->access_history.time_stamp[a->access_history.count] - a->access_history.time_stamp[0])/a->access_history.count; 
    fb = (b->access_history.time_stamp[b->access_history.count] - b->access_history.time_stamp[0])/b->access_history.count; 
    agb = (fa < fb); 
    break; 
  default:
    printf("Unknown cache replacement policy %d; use LRU (least recently used)\n", replacement_policy); 
    agb = (a->access_history.time_stamp[a->access_history.count] 
	   < b->access_history.time_stamp[b->access_history.count]); 
    break; 
  } 
  return agb; 
}

/* H5LSclaim_space trying to claim a portionof space for a cache. 
          LS - the local storage struct 
        size - the size of the space in bytes
        type - claim type [HARD / SOFT]
 */

herr_t H5LSclaim_space(LocalStorage *LS, hsize_t size, cache_claim_t type, cache_replacement_policy_t crp) {
    if (LS->mspace_left > size) {
        LS->mspace_left = LS->mspace_left - size;  
        return SUCCEED;
    } else {
        if (type == SOFT) {
          return FAIL; 
        } else {  
	  double mspace = 0.0;
	  /// compute the total space for all the temporal cache; 
	  CacheList *head  = LS->cache_list;
	  LocalStorageCache *tmp, *stay; 
          while(head!=NULL) {
	    if (head->cache->duration==TEMPORAL) {
	      mspace += head->cache->mspace_total;
	      tmp = head->cache; 
	    }
            head = head->next; 
	  }
	  stay = tmp; 
	  if (mspace < size)
	    return FAIL;
	  else {
	    mspace = 0.0;
	    while(mspace < size) {
	      head = LS->cache_list;
	      while(head!=NULL) {
		if ((head->cache->duration==TEMPORAL) && H5LScompare_cache(head->cache, tmp, crp)) {
		  tmp = head->cache;
		  stay = tmp; 
		}
		if (mspace > 0) continue; // if already found one, as long as we find another one that is 
		head = head->next; 
	      }
	      mspace += tmp->mspace_total;
	      H5LSremove_cache(LS, tmp);
	      tmp = stay; 
	    }
	  }
        }
    }
    return 0; 
}

/*
  Clear certain cache
 */
herr_t H5LSremove_cache(LocalStorage *LS, LocalStorageCache *cache) {
  if (LS->io_node && LS->storage!=MEMORY) {
    DIR *theFolder = opendir(cache->path);
    if (debug_level()>1) printf("cache->path: %s\n", cache->path); 
    struct dirent *next_file;
    char filepath[256];
    while ( (next_file = readdir(theFolder)) != NULL ) {
      // build the path for each file in the folder
      sprintf(filepath, "%s/%s", cache->path, next_file->d_name);
      if (debug_level()>1) printf("remove_cache filepath: %s\n", filepath);
      remove(filepath);
    }
    closedir(theFolder);
    rmdir(cache->path);
  }
  
  CacheList *head = LS->cache_list;
  while (head !=NULL && head->cache != cache ) {
    head = head->next; 
  }
  if (head !=NULL && head->cache !=NULL && head->cache == cache) {
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *) head->target;    o->write_cache = false; 
    o->read_cache = false;
  }
  free(cache);
  if (head !=NULL) head=head->next; 
  return 0; 
}

/* 
   clear all the cache all at once
 */
herr_t H5LSremove_cache_all(LocalStorage *LS) {
  CacheList *head  = LS->cache_list;
  while(head!=NULL) {
    if (LS->io_node) {
      DIR *theFolder = opendir(head->cache->path);
      struct dirent *next_file;
      char filepath[256];
      while ( (next_file = readdir(theFolder)) != NULL ) {
	if (debug_level()>1) sprintf(filepath, "%s/%s", head->cache->path, next_file->d_name);
	remove(filepath);
      }
      closedir(theFolder);
      rmdir(head->cache->path);
      free(head->cache);
      head = head->next; 
    }
  }
 return 0; 
}

/* Register certain cache to the list 
  */
herr_t H5LSregister_cache(LocalStorage *LS, LocalStorageCache *cache, void *target) {
  CacheList *head = LS->cache_list;
  LS->cache_list = (CacheList*) malloc(sizeof(CacheList)); 
  LS->cache_list->cache = cache;
  LS->cache_list->target = target; 
  LS->cache_list->next = head;
  cache->access_history.time_stamp[0] = time(NULL);
  cache->access_history.count = 0; 
  return SUCCEED; 
}


herr_t H5LSrecord_cache_access(LocalStorageCache *cache) {
  cache->access_history.count++;
  if (cache->access_history.count < MAX_NUM_CACHE_ACCESS) {
    cache->access_history.time_stamp[cache->access_history.count] = time(NULL);
  } else {
    // if overflow, we only record the most recent one at the end
    cache->access_history.time_stamp[cache->access_history.count%MAX_NUM_CACHE_ACCESS] = time(NULL);
  }
  return SUCCEED; 
};

