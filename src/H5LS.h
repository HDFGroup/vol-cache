#ifndef H5LS_H__
#define H5LS_H__
#include "stdio.h"
#include "stdlib.h"
#include "hdf5.h"
#include "time.h"
#include "mpi.h"
#define MAX_NUM_CACHE_FILE 1000
#define MAX_NUM_CACHE_ACCESS 1000
// various enum to define 
enum cache_purpose {READ, WRITE, RDWR};
enum cache_duration {PERMANENT, TEMPORAL};
enum cache_claim {SOFT, HARD};
enum cache_replacement_policy {FIFO, LIFO, LRU, LFU};

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
  hsize_t mspace_total; // total space available per node
  hsize_t mspace_left;  // space left per node 
  hsize_t mspace_per_rank_total; // total space per process
  hsize_t mspace_per_rank_left;  // space left per process
  hid_t fd; // the associate file
  char path[255]; // path 
  AccessHistory access_history; 
} LocalStorageCache;

typedef struct _thread_data_t {
  // we will use the link structure in C to build the list of I/O tasks
  char fname[255];
  void *dataset_obj;
  hid_t dataset_id; 
  hid_t mem_type_id; 
  hid_t mem_space_id; 
  hid_t file_space_id; 
  hid_t xfer_plist_id;
  void *req; 
  void *h5_state; 
  int id;
  hsize_t offset; // offset in memory mapped file on SSD
  hsize_t size; 
  void *buf; 
  struct _thread_data_t *next; 
} thread_data_t;

// MPI infos 
typedef struct _MPI_INFO {
  int rank;
  int nproc; 
  int local_rank; // rank id in the local communicator
  int ppn; // number or processors in the 
  MPI_Comm comm, comm_t; // global communicator
  MPI_Comm node_comm; // node local communicator
  MPI_Win win, win_t;
  hsize_t offset; 
} MPI_INFO; 

// I/O threads 
typedef struct _IO_THREAD {
  int num_request; // for parallel write
  thread_data_t *request_list, *current_request, *first_request; // task queue
  bool batch_cached; // for parallel read, -- whether the batch data is cached to SSD or not
  bool dset_cached; // whether the entire dataset is cached to SSD or not.
  hsize_t offset_current; 
  int round; 
} IO_THREAD;

// Memory mapped files 
typedef struct _MMAP {
  // for write
  int fd; // file handle for write
  char fname[255];// full path of the memory mapped file
  void *buf; // pointer that map the file to the memory
  void *tmp_buf; // temporally buffer, used for parallel read: copy the read buffer, return the H5Dread_to_cache function, the back ground thread write the data to the SSD. 
  hsize_t offset; 
} MMAP; 

// Dataset 
typedef struct _SAMPLE {
  size_t dim; // the number of dimension
  size_t size; // the size of the sample in bytes. 
  size_t nel; // the number of elements per sample, 
} SAMPLE; 

typedef struct _BATCH {
  int *list;
  int size; 
} BATCH;

typedef struct _DSET {
  SAMPLE sample; 
  size_t ns_loc; // number of samples per rank
  size_t ns_glob; // total number of samples
  size_t s_offset; // offset
  hsize_t size; // the size of dataset in bytes (per rank). 
  BATCH batch; // batch data to read
  int ns_cached; 
  bool contig_read; // whether the batch of data to read is contigues or not. 
  MPI_Datatype mpi_datatype; // the constructed mpi dataset
  hid_t h5_datatype; // hdf5 dataset
  size_t esize; // the size of an element in bytes. 
} DSET; 

/* 
   This define the storage to use. 
 */
typedef struct _CacheList {
  LocalStorageCache *cache;
  void *target; // the target file/dataset for the cache
  struct _CacheList *next;
} CacheList;


typedef struct H5LS_cache_io_class_t {
  char type[255];
  herr_t (*create_file_cache_on_storage)(void *obj, const char *name, hid_t fapl_id, cache_purpose_t purpose, cache_duration_t duration);
  herr_t (*remove_file_cache_on_storage)(void *file);
  herr_t (*create_dataset_cache_on_storage)(void *obj, const char *name);
  herr_t (*remove_dataset_cache_on_storage)(void *obj);
  void *(*write_data_to_storage)(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf);
  herr_t (*read_data_from_storage)(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf);
} H5LS_cache_io_class_t;

typedef struct H5LS_mmap_class_t {
  char  type[255]; 
  herr_t (*create_write_mmap)(MMAP *mmap, hsize_t size);
  herr_t (*remove_write_mmap)(MMAP *mmap, hsize_t size);
  void *(*write_buffer_to_mmap)(hid_t mem_space_id, hid_t mem_type_id, const void *buf, hsize_t size, MMAP *mmap);
  herr_t (*create_read_mmap)(MMAP *mmap, hsize_t size);
  herr_t (*remove_read_mmap)(MMAP *mmap, hsize_t size);
  herr_t (*removeCacheFolder) (const char *path);
} H5LS_mmap_class_t; 

typedef struct _LocalStorage {
  char type[255];
  char *path;
  hsize_t mspace_total;
  hsize_t mspace_left;
  CacheList *cache_list; 
  int num_cache;
  bool io_node;  // select I/O node for I/O
  double write_cache_size; 
  cache_replacement_policy_t replacement_policy;
  const H5LS_mmap_class_t *mmap_cls;
  const H5LS_cache_io_class_t *cache_io_cls; 
  // some function
  //
} LocalStorage; 

typedef struct _H5Dwrite_cache_metadata {
  MMAP mmap;
  MPI_INFO mpi; 
  IO_THREAD io;
  LocalStorageCache *cache;
  LocalStorage *H5LS; 
} H5Dwrite_cache_metadata; 

typedef struct _H5Dread_cache_metadata {
  MMAP mmap;
  MPI_INFO mpi;
  IO_THREAD io;
  DSET dset;
  void *h5_state; 
  LocalStorageCache *cache;
  LocalStorage *H5LS; 
} H5Dread_cache_metadata;

#ifdef __cplusplus
extern "C" {
#endif
  const H5LS_mmap_class_t* get_H5LS_mmap_class_t(char *type);
  herr_t readLSConf(char *fname, LocalStorage *LS);
  cache_replacement_policy_t get_replacement_policy_from_str(char *str); 
  herr_t H5LSset(LocalStorage *LS, char *type, char *path, hsize_t avail_space, cache_replacement_policy_t t);
  herr_t H5LSclaim_space(LocalStorage *LS, hsize_t size, cache_claim_t type, cache_replacement_policy_t crp);
  herr_t H5LSremove_cache_all(LocalStorage *LS);
  herr_t H5LSregister_cache(LocalStorage *LS, LocalStorageCache *cache, void *target);
  herr_t H5LSremove_cache(LocalStorage *LS, LocalStorageCache *cache);
  herr_t H5LSrecord_cache_access(LocalStorageCache *cache);
  herr_t H5LSget(LocalStorage *LS, char *flag, void *value);
  LocalStorage *H5LScreate(hid_t plist); // in future, maybe we can consider to have a hid_t;
  herr_t H5Pset_fapl_cache(hid_t plist, char *flag, void *value);
  herr_t H5Pget_fapl_cache(hid_t plist, char *flag, void *value);
#ifdef __cplusplus
}
#endif
#endif
