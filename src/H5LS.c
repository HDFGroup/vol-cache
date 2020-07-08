#include "H5LS.h"
#include <sys/types.h> 
#include <sys/stat.h> 
#include <unistd.h> 
#include <stdio.h> 
#include <stdlib.h> 
#include <string.h>
#include <dirent.h>
#ifndef FAIL
#define FAIL -1
#endif
#ifndef SUCCEED
#define SUCCEED 0
#endif
herr_t H5LSset(LocalStorage *LS, cache_storage_t storage, char *path, hsize_t mspace_total)
{
    LS->storage = storage;
    LS->mspace_total = mspace_total;
    LS->mspace_left = mspace_total; 
    LS->num_cache = 0; 
    strcpy(LS->path, path);//check existence of the space
    return 0; 
}

herr_t H5LSclaim_space(LocalStorage *LS, hsize_t size, cache_claim_t type) {
    if (LS->mspace_left > size) {
        LS->mspace_left = LS->mspace_left - size;  
        return SUCCEED;
    } else {
        if (type == SOFT) {
          return FAIL; 
        } else {
          printf("NOT IMPLEMENTED YET\n");
          return FAIL; 
        }
    }
    return 0; 
}

/*
  Clear certain cache
 */
herr_t H5LSremove_cache(LocalStorage *LS, LocalStorageCache *cache) {
  DIR *theFolder = opendir(cache->path);
  struct dirent *next_file;
  char filepath[256];
  while ( (next_file = readdir(theFolder)) != NULL ) {
    // build the path for each file in the folder
    sprintf(filepath, "%s/%s", cache->path, next_file->d_name);
    remove(filepath);
  }
  closedir(theFolder);
  CacheList *head = LS->cache_list;
  while (head->cache != cache && head !=NULL) {
     head = head->next; 
  }
  head=head->next; 
  free(cache);
  return 0; 
}

/* 
   clear all the cache all at once
 */
herr_t H5LSremove_cache_all(LocalStorage *LS) {
  CacheList *head  = LS->cache_list;
  while(head!=NULL) {
      DIR *theFolder = opendir(head->cache->path);
      struct dirent *next_file;
      char filepath[256];
      while ( (next_file = readdir(theFolder)) != NULL ) {
        sprintf(filepath, "%s/%s", head->cache->path, next_file->d_name);
        remove(filepath);
      }
      closedir(theFolder);
      free(head->cache);
      head = head->next; 
 }
 return 0; 
}

herr_t H5LSregister_cache(LocalStorage *LS, LocalStorageCache *cache) {
  CacheList *head = LS->cache_list;
  LS->cache_list = (CacheList*) malloc(sizeof(CacheList)); 
  LS->cache_list->cache = cache; 
  LS->cache_list->next = head;
  return 0; 
}
