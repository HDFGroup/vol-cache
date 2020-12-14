/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://support.hdfgroup.org/ftp/HDF5/releases.  *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Purpose:	The public header file for the pass-through VOL connector.
 */

#ifndef _H5VLcache_ext_H
#define _H5VLcache_ext_H
#include "mpi.h"
#include "cache_utils.h"
/* Public headers needed by this file */
#include "H5VLpublic.h"        /* Virtual Object Layer                 */
#include "H5LS.h"
/* Identifier for the pass-through VOL connector */
#define H5VL_CACHE_EXT	(H5VL_cache_ext_register())

/* Characteristics of the pass-through VOL connector */
#define H5VL_CACHE_EXT_NAME        "cache_ext"
#define H5VL_CACHE_EXT_VALUE       518           /* VOL connector ID */
#define H5VL_CACHE_EXT_VERSION     0


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

typedef struct _DSET {
  SAMPLE sample; 
  size_t ns_loc; // number of samples per rank
  size_t ns_glob; // total number of samples
  size_t s_offset; // offset
  hsize_t size; // the size of the entire dataset in bytes. 
  BATCH batch; // batch data to read
  int ns_cached; 
  bool contig_read; // whether the batch of data to read is contigues or not. 
  MPI_Datatype mpi_datatype; // the constructed mpi dataset
  hid_t h5_datatype; // hdf5 dataset
  size_t esize; // the size of an element in bytes. 
} DSET; 

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


#ifndef H5VL_STRUCT
#define H5VL_STRUCT
/* Pass-through VOL connector info */
typedef struct H5VL_cache_ext_info_t {
    hid_t under_vol_id;         /* VOL ID for under VOL */
    void *under_vol_info;       /* VOL info for under VOL */
    char fconfig[255];          /* file name for config */
} H5VL_cache_ext_info_t;

/* The pass through VOL info object */
typedef struct H5VL_cache_ext_t {
    hid_t  under_vol_id;        /* ID for underlying VOL connector */
    void  *under_object;       /* Info object for underlying VOL connector */
    H5Dread_cache_metadata *H5DRMM;
    H5Dwrite_cache_metadata *H5DWMM;
    bool read_cache;
    bool read_cache_info_set; 
    bool write_cache;
    bool write_cache_info_set; 
    int num_request_dataset;
    void *parent; 
} H5VL_cache_ext_t;

/* The cache VOL wrapper context */
typedef struct H5VL_cache_ext_wrap_ctx_t {
    hid_t under_vol_id;         /* VOL ID for under VOL */
    void *under_wrap_ctx;       /* Object wrapping context for under VOL */
} H5VL_cache_ext_wrap_ctx_t;
#endif

#define H5P_LOCAL_STORAGE_CREATE (H5OPEN H5P_CLS_LOCAL_STORAGE_CREATE_ID_g)
H5_DLLVAR hid_t H5P_CLS_LOCAL_STORAGE_CREATE_ID_g; 


#endif /* _H5VLcache_H */

