/* 
   This source file contains a set of function for management the node-local storage. 
 */
#include "H5LS.h"
#include <sys/types.h> 
#include <sys/stat.h> 
#include <unistd.h> 
#include <stdio.h> 
#include <stdlib.h> 
#include <string.h>
#include <dirent.h>
#include "H5VLcache_ext.h"

#include "debug.h"
#ifndef FAIL
#define FAIL -1
#endif
#ifndef SUCCEED
#define SUCCEED 0
#endif

#ifndef STDERR
#ifdef __APPLE__
#define STDERR __stderrp
#else
#define STDERR stderr
#endif
#endif


/*-------------------------------------------------------------------------
 * Function:    H5Pset_fapl_cache
 *
 * Purpose:     Set local storage related property to file access property list
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t H5Pset_fapl_cache(hid_t plist, char *flag, void *value) {
  herr_t ret;
  size_t s = 1; 
  if (strcmp(flag, "HDF5_CACHE_WR")==0 || !strcmp(flag, "HDF5_CACHE_RD")==0) s = sizeof(bool);
  if (strcmp(flag, "LOCAL_STORAGE")==0) s = sizeof(LocalStorage);
  if (strcmp(flag, "HDF5_WRITE_CACHE_SIZE")==0) s = sizeof(hsize_t); 
  if (strcmp(flag, "HDF5_CACHE_WR")==0 ||
      strcmp(flag, "HDF5_CACHE_RD")==0 ||
      strcmp(flag, "HDF5_WRITE_CACHE_SIZE")==0 ||
      strcmp(flag, "LOCAL_STORAGE")==0) {
    if (H5Pexist(plist, flag)==0) 
      ret = H5Pinsert2(plist, flag, s, value, NULL, NULL, NULL, NULL, NULL, NULL);
    else
      ret = H5Pset(plist, flag, value);
  } else {
    fprintf(STDERR, "ERROR in H5Pset_fapl_cache: property list does not have property: %s", flag); 
    ret = FAIL; 
  }
  return ret; 
} /* end H5Pset_fapl_cache() */


/*-------------------------------------------------------------------------
 * Function:    H5Pget_fapl_cache
 *
 * Purpose:     Get local storage related property to file access property list
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t H5Pget_fapl_cache(hid_t plist, char *flag, void *value) {
  herr_t ret;
  if (H5Pexist(plist, flag)>0)
    ret = H5Pget(plist, flag, value);
  else {
    ret = FAIL;
  }
  return ret; 
} /* end H5Pget_fapl_cache() */
 


/*-------------------------------------------------------------------------
 * Function:    H5LSset 
 *
 * Purpose:     set the global local storage property
 * 
 * Input: 
 *           LS - the local storage struct 
 *      storage - the type of storage [SSD, BURST_BUFFER, MEMORY]
 *         path - the path to the local storage
 * mspace_total - the capacity of the local storage in Bytes. 
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t H5LSset(LocalStorage *LS, cache_storage_t storage, char *path, hsize_t mspace_total, cache_replacement_policy_t replacement)
{
#ifdef ENABLE_EXT_CACHE_LOGGING
  printf("------- EXT CACHE H5LSset\n"); 
#endif
    LS->storage = storage;
    LS->mspace_total = mspace_total;
    LS->mspace_left = mspace_total; 
    LS->num_cache = 0;
    LS->replacement_policy = replacement; 
    struct stat sb;
    strcpy(LS->path, path);//check existence of the space
    if (storage == MEMORY || ( stat(path, &sb) == 0 && S_ISDIR(sb.st_mode))) {
      return 0; 
    } else {
      fprintf(STDERR, "ERROR in H5LSset: %s does not exist\n", path); 
      exit(EXIT_FAILURE); 
    }
} /* end H5LSset */



/*-------------------------------------------------------------------------
 * Function:    H5LSget
 *
 * Purpose:     get the global local storage property
 * 
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t H5LSget(LocalStorage *LS, char *flag, void *value) {
#ifdef ENABLE_EXT_CACHE_LOGGING
  printf("------- EXT CACHE H5LSget\n"); 
#endif

  if (strcmp(flag, "TYPE")==0) value = &LS->storage;
  else if (strcmp(flag, "PATH")==0) value=&LS->path;
  else if (strcmp(flag, "SIZE")==0) value=&LS->mspace_total;
  else {
    return FAIL; 
  }
  return SUCCEED; 
} /* end H5LSget() */



/*-------------------------------------------------------------------------
 * Function:    H5LScreate
 *
 * Purpose:     Create a LocalStorage using H5P_LOCAL_STORAGE_CREATE property list
 * 
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
LocalStorage *H5LScreate(hid_t plist) {
#ifdef ENABLE_EXT_CACHE_LOGGING
  printf("------- EXT CACHE H5LScreate\n"); 
#endif
  LocalStorage *LS = (LocalStorage *) malloc(sizeof(LocalStorage));
  cache_storage_t storage;
  char path[255];
  hsize_t mspace_total;
  cache_replacement_policy_t replacement;
  H5Pget(plist, "TYPE", &storage);
  H5Pget(plist, "PATH", &path);
  H5Pget(plist, "REPLACEMENT_POLICY", &replacement);
  H5Pget(plist, "SIZE", &mspace_total);
  H5LSset(LS, storage, path, mspace_total, replacement);
  return LS; 
} /* end H5LScreate */


/*-------------------------------------------------------------------------
 * Function:    H5LScompare_cache
 *
 * Purpose:     Compare the two cache
 * 
 * Return:      true (a > b), or false (b > a)
 *
 *-------------------------------------------------------------------------
 */
bool H5LScompare_cache(LocalStorageCache *a, LocalStorageCache *b, cache_replacement_policy_t replacement_policy) {
#ifdef ENABLE_EXT_CACHE_LOGGING
  printf("------- EXT CACHE H5LScompare_cache\n"); 
#endif
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
} /* end H5LScompare_cache() */


/*-------------------------------------------------------------------------
 *  Function: H5LSclaim_space 
 *  Purpose: trying to claim a portionof space for a cache. 
 *  Input: 
 *         LS - the local storage struct 
 *       size - the size of the space in bytes
 *       type - claim type [HARD / SOFT]
 *  Return:  0 / -1
 *-------------------------------------------------------------------------
 */
herr_t H5LSclaim_space(LocalStorage *LS, hsize_t size, cache_claim_t type, cache_replacement_policy_t crp) {
#ifdef ENABLE_EXT_CACHE_LOGGING
  printf("------- EXT CACHE H5LSclaim_space\n"); 
#endif
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


/*-------------------------------------------------------------------------
 *  Function: H5LSremove_cache
 *  Purpose: Clear certain cache, remove all the files associated with it.  
 *-------------------------------------------------------------------------
 */
herr_t H5LSremove_cache(LocalStorage *LS, LocalStorageCache *cache) {
#ifdef ENABLE_EXT_CACHE_LOGGING
  printf("------- EXT CACHE H5LSremove_space\n"); 
#endif
  if (cache!=NULL) {
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
      H5VL_cache_ext_t *o = (H5VL_cache_ext_t *) head->target;
      o->write_cache = false; 
      o->read_cache = false;
    }
    LS->mspace_left += cache->mspace_total; 
    free(cache);
    cache = NULL;
    if (head !=NULL) head=head->next;
  } else {
    if (LS->io_node) printf("Trying to remove nonexisting cache\n"); 
  }
  return 0; 
} /* end H5LSremove_cache() */


/*-------------------------------------------------------------------------
 *  Function: H5LSremove_cache_all
 *  Purpose: Clear all cache, remove all the files associated with it.  
 *-------------------------------------------------------------------------
 */
herr_t H5LSremove_cache_all(LocalStorage *LS) {
#ifdef ENABLE_EXT_CACHE_LOGGING
  printf("------- EXT CACHE H5LSremove_space_all\n"); 
#endif
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
} /* end H5LSremove_cache_all() */


/*-------------------------------------------------------------------------
 *  Function: H5LSregister_cache
 *  Purpose:  register the cache to the local storage  
 *-------------------------------------------------------------------------
 */
herr_t H5LSregister_cache(LocalStorage *LS, LocalStorageCache *cache, void *target) {
#ifdef ENABLE_EXT_CACHE_LOGGING
  printf("------- EXT CACHE H5LSregister_cache\n"); 
#endif
  CacheList *head = LS->cache_list;
  LS->cache_list = (CacheList*) malloc(sizeof(CacheList)); 
  LS->cache_list->cache = cache;
  LS->cache_list->target = target; 
  LS->cache_list->next = head;
  cache->access_history.time_stamp[0] = time(NULL);
  cache->access_history.count = 0; 
  return SUCCEED; 
} /* end H5LSregister_cache() */



/*-------------------------------------------------------------------------
 *  Function: H5LSrecord_cache_access
 *
 *  Purpose:  Record the access event for the cache 
 *
 *-------------------------------------------------------------------------
 */
herr_t H5LSrecord_cache_access(LocalStorageCache *cache) {
#ifdef ENABLE_EXT_CACHE_LOGGING
  printf("------- EXT CACHE H5LSrecore_cache_acess\n"); 
#endif
  cache->access_history.count++;
  if (cache->access_history.count < MAX_NUM_CACHE_ACCESS) {
    cache->access_history.time_stamp[cache->access_history.count] = time(NULL);
  } else {
    // if overflow, we only record the most recent one at the end
    cache->access_history.time_stamp[cache->access_history.count%MAX_NUM_CACHE_ACCESS] = time(NULL);
  }
  return SUCCEED; 
} /* end H5LSrecord_cache_access() */

