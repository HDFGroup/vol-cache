/*
  This is the header files for node local storage incorporated HDF5
 */
#ifndef H5DIO_CACHE_H_
#define H5DIO_CACHE_H_
#include "hdf5.h"
#include "mpi.h"
#include "H5LS.h"
#ifndef MAXDIM
#define MAXDIM 32
#endif
// The meta data for I/O thread to perform parallel write
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

/************************************** 
 *  Function APIs for parallel write  *
 **************************************/
// Create HDF5 file: create memory mapped file on the SSD
#ifdef __cplusplus
extern "C" {
#endif
// set hyperslab selection given the sample list
void set_hyperslab_from_samples(int *samples, int nsample, hid_t *fspace);
// get the list of the samples from the filespace
void get_samples_from_filespace(hid_t fspace, BATCH *samples, bool *contiguous);
// get the buffer size from the mspace and type ids.
hsize_t get_buf_size(hid_t mspace, hid_t tid);
void parallel_dist(size_t dim, int nproc, int rank, size_t *ldim, size_t *start);
void int2char(int a, char str[255]);
#ifdef __cplusplus
}
#endif
#endif //H5Dio_cache.h
