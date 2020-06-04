/*
   This is for the prototype design of using node local storage
   to improve parallel I/O performance. 
   
   For parallel write: 
   We created the H5Dwrite_cache function so that the data will 
   write to the local SSD first and then the background thread 
   will take care of the data migration from the local SSD to the file system. 
   We create a pthread for doing I/O work using a first-in-first-out 
   framework. 
   
   For parallel read: 
   We created H5Dread_to_cache and H5Dread_from_cache functions. The 
   first one will read the data from the parallel file system and then 
   save the data to the node local storage. H5Dread_from_cache from read 
   data directly from the node local storage. 
   
   Notice that in order for this to work, one has to set
   * MPI_Init_thread(..., ..., MPI_THREAD_MULTIPLE, ...)
   
   Huihuo Zheng <huihuo.zheng@anl.gov>
   April 24, 2020
 */
#include <pthread.h>
#include "mpi.h"
#include "stdlib.h"
#include "string.h"
#include "unistd.h"
// POSIX I/O
#include "sys/stat.h"
#include <fcntl.h>
#include "H5Dio_cache.h"

// Memory map
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/statvfs.h>

/***********/
/* HDF5 Headers */
/***********/

#include "hdf5.h"
#include "H5FDmpio.h"


// Debug 
#include "debug.h"
/* 
   Global variables to define information related to the local storage
*/
#define MAXDIM 32
#define  PAGESIZE sysconf(_SC_PAGE_SIZE)

// initialize H5DWMM data
SSD_INFO SSD;
H5Dwrite_cache_metadata H5DWMM;
/*
  Function for set up the local storage path and capacity.
*/
void setH5SSD(SSD_INFO *ssd) {
  // set ssd_PATH
  printf("SSD_PATH\n"); 
  if (getenv("SSD_PATH")) {
    strcpy(ssd->path, getenv("SSD_PATH"));
  } else {
    strcpy(ssd->path, "/local/scratch/");
  }
  // set SSD_SIZE;
  printf("SSD_SIZE\n"); 
  if (getenv("SSD_SIZE")) {
    ssd->mspace_total = atof(getenv("SSD_SIZE"))*1024*1024*1024;
    ssd->mspace_left = ssd->mspace_total; 
  } else {
    ssd->mspace_total = 137438953472;
    ssd->mspace_left = 137438953472;
  }
  printf("SSD_SIZE done\n"); 
  ssd->offset = 0; 
}
void int2char(int a, char str[255]) {
  sprintf(str, "%d", a);
}

/*
  Purpose: get the size of the buffer from the memory space and type id
  Output: the size of the memory space in bytes. 
 */
hsize_t get_buf_size(hid_t mspace, hid_t tid) {
  hsize_t nelement = H5Sget_select_npoints(mspace);
  hsize_t s = H5Tget_size(tid);
  return s*nelement;
}


/*
  Thread function for performing H5Dwrite. This function will create 
  a memory mapped buffer to the file that is on the local storage which 
  contains the data to be written to the file system. 

  On Theta, the memory mapped buffer currently does not work with H5Dwrite, we instead allocate a buffer directly to the memory. 
*/
void *H5Dwrite_pthread_func(void *arg) {
  // this is to us the H5DWMM as an input
  H5Dwrite_cache_metadata *wmm = (H5Dwrite_cache_metadata*) arg;
  pthread_mutex_lock(&wmm->io.request_lock);
  bool loop= (wmm->io.num_request>=0);
  bool working = (wmm->io.num_request>0);
  bool done = (wmm->io.num_request==0);
  pthread_mutex_unlock(&wmm->io.request_lock);
  while (loop) {
    if (working) {
      thread_data_t *data = wmm->io.current_request;
      data->buf = mmap(NULL, data->size, PROT_READ, MAP_SHARED, wmm->mmap.fd, data->offset);
      msync(data->buf, data->size, MS_SYNC);
#ifdef THETA
      wmm->mmap.tmp_buf = malloc(data->size);
      memcpy(wmm->mmap.tmp_buf, data->buf, data->size); 
      H5Dwrite(data->dataset_id, data->mem_type_id, 
	       data->mem_space_id, data->file_space_id, 
	       data->xfer_plist_id, wmm->mmap.tmp_buf);
      free(wmm->mmap.tmp_buf);
#else
      H5Dwrite(data->dataset_id, data->mem_type_id, 
	       data->mem_space_id, data->file_space_id, 
	       data->xfer_plist_id, data->buf);
#endif
      H5Sclose(data->mem_space_id);
      H5Sclose(data->file_space_id);
      H5Tclose(data->mem_type_id);
      munmap(data->buf, data->size);
      wmm->io.current_request=wmm->io.current_request->next;
      wmm->io.num_request--;
      if (io_node()==wmm->mpi.rank) printf("wmm: %d\n", wmm->io.current_request->id);
    }
    pthread_mutex_lock(&wmm->io.request_lock);
    loop= (wmm->io.num_request>=0);
    working = (wmm->io.num_request>0);
    done = (wmm->io.num_request==0);
    pthread_mutex_unlock(&wmm->io.request_lock);
    if (done) {
      pthread_mutex_unlock(&wmm->io.request_lock);
      pthread_cond_signal(&wmm->io.master_cond);
      pthread_cond_wait(&wmm->io.io_cond, &wmm->io.request_lock);
      pthread_mutex_unlock(&wmm->io.request_lock);
    }
  }
  return NULL; 
}

/* 
   Create HDF5 file. Each rank will create a file on the local storage
   for temperally store the data to be written to the file system. 
   We also create a local communicator including all the processes on the node.
   A pthread is created for migrating data from the local storage to the 
   file system asynchonously. 
   
   The function return directly without waiting the I/O thread to finish 
   the I/O task. However, if the space left on the local storage is not 
   enough for storing the buffer of the current task, it will wait for the 
   I/O thread to finsh all the previous tasks.
 */
hid_t H5Fcreate_cache( const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id ) {
  H5DWMM.io.num_request = 0;
  pthread_cond_init(&H5DWMM.io.io_cond, NULL);
  pthread_cond_init(&H5DWMM.io.master_cond, NULL);
  pthread_mutex_init(&H5DWMM.io.request_lock, NULL);
  srand(time(NULL));   // Initialization, should only be called once.
  setH5SSD(&SSD);
  H5DWMM.ssd = &SSD; 
  MPI_Comm comm, comm_dup;
  MPI_Info info; 
  H5Pget_fapl_mpio(fapl_id, &comm, &info);
  MPI_Comm_dup(comm, &H5DWMM.mpi.comm);
  MPI_Comm_rank(comm, &H5DWMM.mpi.rank);
  MPI_Comm_size(comm, &H5DWMM.mpi.nproc);
  MPI_Comm_split_type(H5DWMM.mpi.comm, MPI_COMM_TYPE_SHARED, 0, MPI_INFO_NULL, &H5DWMM.mpi.node_comm);
  MPI_Comm_rank(H5DWMM.mpi.node_comm, &H5DWMM.mpi.local_rank);
  MPI_Comm_size(H5DWMM.mpi.node_comm, &H5DWMM.mpi.ppn);
  strcpy(H5DWMM.mmap.fname, H5DWMM.ssd->path);
  char rnd[255];
  sprintf(rnd, "%d", rand());
  strcat(H5DWMM.mmap.fname, rnd);
  strcat(H5DWMM.mmap.fname, "-"); 
  sprintf(rnd, "%d", H5DWMM.mpi.rank);
  strcat(H5DWMM.mmap.fname, rnd);
  H5DWMM.io.request_list = (thread_data_t*) malloc(sizeof(thread_data_t)); 
  H5DWMM.ssd->mspace_per_rank_total = H5DWMM.ssd->mspace_total / H5DWMM.mpi.ppn;
  H5DWMM.ssd->mspace_per_rank_left = H5DWMM.ssd->mspace_per_rank_total;
  H5DWMM.mmap.fd = open(H5DWMM.mmap.fname, O_RDWR | O_CREAT | O_TRUNC, 0644);
  int rc = pthread_create(&H5DWMM.io.pthread, NULL, H5Dwrite_pthread_func, &H5DWMM);
  pthread_mutex_lock(&H5DWMM.io.request_lock);
  H5DWMM.io.request_list->id = 0; 
  H5DWMM.io.current_request = H5DWMM.io.request_list; 
  H5DWMM.io.first_request = H5DWMM.io.request_list;
  pthread_mutex_unlock(&H5DWMM.io.request_lock);

  return H5Fcreate(name, flags, fcpl_id, fapl_id);
}

/*
  This is the write function appears to the user. 
  The function arguments are the same with H5Dwrite. 
  This function writes the buffer to the local storage
  first and It will create an I/O task and add it to the task 
  lists, and then wake up the I/O thread to execute 
  the H5Dwrite function. 
*/
herr_t
H5Dwrite_cache(hid_t dataset_id, hid_t mem_type_id, hid_t mem_space_id,
	       hid_t file_space_id, hid_t dxpl_id, const void *buf) {
  hsize_t size = get_buf_size(mem_space_id, mem_type_id);
  if (H5DWMM.ssd->mspace_per_rank_left < size) {
    H5WPthreadWait();
    H5DWMM.ssd->offset = 0;
    H5DWMM.ssd->mspace_per_rank_left = H5DWMM.ssd->mspace_per_rank_total;
    H5DWMM.ssd->mspace_left = H5DWMM.ssd->mspace_total; 
  }
  int err = pwrite(H5DWMM.mmap.fd, (char*)buf, size, H5DWMM.ssd->offset);
  H5DWMM.io.request_list->offset = H5DWMM.ssd->offset; 
  H5DWMM.ssd->offset += (size/PAGESIZE+1)*PAGESIZE;
  H5DWMM.ssd->mspace_per_rank_left = H5DWMM.ssd->mspace_per_rank_total
    - H5DWMM.ssd->offset*H5DWMM.mpi.ppn;
#ifdef __APPLE__
  fcntl(H5DWMM.mmap.fd, F_NOCACHE, 1);
#else
  fsync(H5DWMM.mmap.fd);
#endif
  H5DWMM.io.request_list->dataset_id = dataset_id; 
  H5DWMM.io.request_list->mem_type_id = H5Tcopy(mem_type_id);
  H5DWMM.io.request_list->mem_space_id = H5Scopy(mem_space_id);
  H5DWMM.io.request_list->file_space_id = H5Scopy(file_space_id);
  H5DWMM.io.request_list->xfer_plist_id = H5Pcopy(dxpl_id);
  if (io_node()==H5DWMM.mpi.rank) printf("building task list %d: \n", H5DWMM.io.request_list->id);
  H5DWMM.io.request_list->size = size; 
  H5DWMM.io.request_list->next = (thread_data_t*) malloc(sizeof(thread_data_t));
  H5DWMM.io.request_list->next->id = H5DWMM.io.request_list->id + 1;
  H5DWMM.io.request_list = H5DWMM.io.request_list->next;
  pthread_mutex_lock(&H5DWMM.io.request_lock);
  H5DWMM.io.num_request++;
  pthread_cond_signal(&H5DWMM.io.io_cond);// wake up I/O thread rightawayx
  pthread_mutex_unlock(&H5DWMM.io.request_lock);
  return err; 
}

/*
  Wait for the pthread to finish all the write request
*/
void H5WPthreadWait() {
  pthread_mutex_lock(&H5DWMM.io.request_lock);
  while(H5DWMM.io.num_request>0)  {
    pthread_cond_signal(&H5DWMM.io.io_cond);
    pthread_cond_wait(&H5DWMM.io.master_cond, &H5DWMM.io.request_lock);
  }
  pthread_mutex_unlock(&H5DWMM.io.request_lock);
}

/*
  Terminate the write pthread through joining
 */
void H5WPthreadTerminate() {
  H5WPthreadWait();
  pthread_mutex_lock(&H5DWMM.io.request_lock);
  H5DWMM.io.num_request=-1;
  pthread_cond_signal(&H5DWMM.io.io_cond);
  pthread_mutex_unlock(&H5DWMM.io.request_lock);
  pthread_join(H5DWMM.io.pthread, NULL);
}

/* 
   Wait for the pthread to finish the work and close the file 
   and terminate the pthread, remove the files on the SSD. 
 */
herr_t H5Fclose_cache( hid_t file_id ) {
  // we should check whether cache is turn on for file_id. Therefore we should have a property. 
  H5WPthreadTerminate();
  close(H5DWMM.mmap.fd);
  remove(H5DWMM.mmap.fname);
  H5DWMM.ssd->mspace_left = H5DWMM.ssd->mspace_total;
  return H5Fclose(file_id);
}

/*
  Wait for pthread to finish the work and close the dataset 
 */
herr_t H5Dclose_cache(hid_t dset_id) {
  H5WPthreadWait();
  return H5Dclose(dset_id);
}
/*
  The following functions are for parallel read.
  
 */
H5Dread_cache_metadata H5DRMM;

/*
  Helper function to compute the local number of samples and the offset. 
 */
void parallel_dist(size_t gdim, int nproc, int rank, size_t *ldim, size_t *start) {
  *ldim = gdim/nproc;
  *start = *ldim*rank; 
  if (rank < gdim%nproc) {
    *ldim += 1;
    *start += rank;
  } else {
    *start += gdim%nproc;
  }
}


/*
  Given a sample list, perform hyperslab selection for filespace; 
 */
void set_hyperslab_from_samples(int *samples, int nsample, hid_t *fspace) {
  static hsize_t gdims[MAXDIM], count[MAXDIM], sample[MAXDIM], offset[MAXDIM];
  int ndims = H5Sget_simple_extent_dims(*fspace, gdims, NULL);
  sample[0] = 1;
  count[0] = 1;
  offset[0] = samples[0]; // set the offset
  for(int i=1; i<ndims; i++) {
    offset[i] = 0;
    sample[i] = gdims[i];
    count[i] = 1;
  }
  H5Sselect_hyperslab(*fspace, H5S_SELECT_SET, offset, NULL, sample, count);
  for(int i=1; i<nsample; i++) {
    offset[0] = samples[i];
    H5Sselect_hyperslab(*fspace, H5S_SELECT_OR, offset, NULL, sample, count);
  }
}
/*
  Get the indices of the samples that have been selected from filespace, and check 
  whether it is contiguous or not. 
*/
void get_samples_from_filespace(hid_t fspace, BATCH *samples, bool *contig) {
  hssize_t numblocks = H5Sget_select_hyper_nblocks(fspace);
  hsize_t gdims[MAXDIM];
  int ndims = H5Sget_simple_extent_dims(fspace, gdims, NULL);
  hsize_t *block_buf = (hsize_t*)malloc(numblocks*2*ndims*sizeof(hsize_t));
  H5Sget_select_hyper_blocklist(fspace, 0, numblocks, block_buf);
  samples->size = 0; 
  for(int i=0; i<numblocks; i++) {
    int start = block_buf[2*i*ndims];
    int end = block_buf[2*i*ndims+ndims];
    for(int j=start; j<end+1; j++) {
      samples->size=samples->size+1;
    }
  }
  samples->list = (int*) malloc(sizeof(int)*samples->size);
  int n=0;
  for(int i=0; i<numblocks; i++) {
    int start = block_buf[2*i*ndims];
    int end = block_buf[2*i*ndims+ndims];
    for(int j=start; j<end+1; j++) {
      samples->list[n] = j;
      n=n+1;
    }
  }

  *contig = H5Sis_regular_hyperslab(fspace);
  free(block_buf);
}


/* 
   pthread function for writing data from memory to the memory
   mapped files on the local storage using MPI_Put
 */
void *H5Dread_pthread_func(void *args) {
  H5Dread_cache_metadata *dmm = (H5Dread_cache_metadata*) args;
  pthread_mutex_lock(&dmm->io.request_lock);
  while (!dmm->io.dset_cached) {
    if (!dmm->io.batch_cached) {
      char *p_mem = (char *) dmm->mmap.tmp_buf;
      MPI_Win_fence(MPI_MODE_NOPRECEDE, dmm->mpi.win);
      int batch_size = dmm->dset.batch.size;
      if (dmm->dset.contig_read) {
	int dest = dmm->dset.batch.list[0];
	int src = dest/dmm->dset.ns_loc;
	MPI_Aint offset = (dest%dmm->dset.ns_loc)*dmm->dset.sample.nel;
	MPI_Put(p_mem, dmm->dset.sample.nel*batch_size,
		dmm->dset.mpi_datatype, src,
		offset, dmm->dset.sample.nel*batch_size,
		dmm->dset.mpi_datatype, dmm->mpi.win);
      } else {
	for(int i=0; i<batch_size; i++) {
	  int dest = dmm->dset.batch.list[i];
	  int src = dest/dmm->dset.ns_loc;
	  MPI_Aint offset = (dest%dmm->dset.ns_loc)*dmm->dset.sample.nel;
	  MPI_Put(&p_mem[i*dmm->dset.sample.size],
		  dmm->dset.sample.nel,
		  dmm->dset.mpi_datatype, src,
		  offset, dmm->dset.sample.nel,
		  dmm->dset.mpi_datatype, dmm->mpi.win);
	}
      }
      MPI_Win_fence(MPI_MODE_NOSUCCEED, dmm->mpi.win);
      if (io_node()==dmm->mpi.rank && debug_level()>2) printf("PTHREAD DONE\n");
      dmm->io.batch_cached = true;
      if (dmm->dset.ns_cached>=dmm->dset.ns_loc)
	dmm->io.dset_cached=true;
    } else {
      pthread_cond_signal(&dmm->io.master_cond);
      pthread_cond_wait(&dmm->io.io_cond, &dmm->io.request_lock); 
    }
  }
  pthread_mutex_unlock(&dmm->io.request_lock);
  return NULL;
}

/*
  Create memory map files on the local storage attach them to MPI_Win
 */
void create_mmap_win(const char *prefix) {
  hsize_t ss = (H5DRMM.dset.size/PAGESIZE+1)*PAGESIZE;
  if (strcmp("MEMORY", getenv("SSD_PATH"))!=0) {
    strcpy(H5DRMM.mmap.fname, H5DRMM.ssd->path);
    strcpy(H5DRMM.mmap.fname, prefix); 
    strcat(H5DRMM.mmap.fname, "-");
    char cc[255];
    int2char(H5DRMM.mpi.rank, cc);
    strcat(H5DRMM.mmap.fname, cc);
    strcat(H5DRMM.mmap.fname, ".dat");
    if (io_node()==H5DRMM.mpi.rank && debug_level() > 1)
      printf(" Creating memory mapped files on local storage: %s\n", H5DRMM.mmap.fname);
    int fh = open(H5DRMM.mmap.fname, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    char a = 'A';
    pwrite(fh, &a, 1, ss);
    fsync(fh);
    close(fh);
    H5DRMM.mmap.fd = open(H5DRMM.mmap.fname, O_RDWR);
    H5DRMM.mmap.buf = mmap(NULL, ss, PROT_READ | PROT_WRITE, MAP_SHARED, H5DRMM.mmap.fd, 0);
    msync(H5DRMM.mmap.buf, ss, MS_SYNC);
  } else {
    if (io_node()==H5DRMM.mpi.rank && debug_level()>1)
      printf(" Allocate buffer in the memory and attached it to a MPI_Win\n");
    H5DRMM.mmap.buf = malloc(ss); 
  }
  MPI_Datatype type[1] = {MPI_BYTE};
  int blocklen[1] = {H5DRMM.dset.esize};
  MPI_Aint disp[1] = {0};
  MPI_Type_create_struct(1, blocklen, disp, type, &H5DRMM.dset.mpi_datatype);
  MPI_Type_commit(&H5DRMM.dset.mpi_datatype);
  MPI_Win_create(H5DRMM.mmap.buf, ss, H5DRMM.dset.esize, MPI_INFO_NULL, H5DRMM.mpi.comm, &H5DRMM.mpi.win);
}

hid_t H5Fopen_cache( const char *name, hid_t fcpl_id, hid_t fapl_id ) {
  srand(time(NULL));   // Initialization, should only be called once.
  setH5SSD(&SSD);
  H5DRMM.ssd = &SSD;
  MPI_Comm comm;
  MPI_Info info; 
  H5Pget_fapl_mpio(fapl_id, &comm, &info);
  MPI_Comm_dup(comm, &H5DRMM.mpi.comm);
  MPI_Comm_rank(comm, &H5DRMM.mpi.rank);
  MPI_Comm_size(comm, &H5DRMM.mpi.nproc);
  MPI_Comm_split_type(H5DRMM.mpi.comm, MPI_COMM_TYPE_SHARED, 0, MPI_INFO_NULL, &H5DRMM.mpi.node_comm);
  MPI_Comm_rank(H5DRMM.mpi.node_comm, &H5DRMM.mpi.local_rank);
  MPI_Comm_size(H5DRMM.mpi.node_comm, &H5DRMM.mpi.ppn);
  H5DRMM.ssd->mspace_per_rank_total = H5DRMM.ssd->mspace_total / H5DRMM.mpi.ppn;
  H5DRMM.ssd->mspace_per_rank_left = H5DRMM.ssd->mspace_per_rank_total;
  return H5Fopen(name, fcpl_id, fapl_id);
}

/*
  Open a dataset, can create memory map.
 */
hid_t H5Dopen_cache(hid_t loc_id, const char *name, hid_t dapl_id) {
  hid_t dset = H5Dopen(loc_id, name, dapl_id);
  pthread_cond_init(&H5DRMM.io.io_cond, NULL);
  pthread_cond_init(&H5DRMM.io.master_cond, NULL);
  pthread_mutex_init(&H5DRMM.io.request_lock, NULL);
  pthread_mutex_lock(&H5DRMM.io.request_lock);
  H5DRMM.io.batch_cached = true;
  H5DRMM.io.dset_cached = false;
  pthread_cond_signal(&H5DRMM.io.io_cond);
  pthread_mutex_unlock(&H5DRMM.io.request_lock);
  srand(time(NULL));   // Initialization, should only be called once.
  H5DRMM.dset.h5_datatype = H5Dget_type(dset);
  H5DRMM.dset.esize = H5Tget_size(H5DRMM.dset.h5_datatype); 
  hid_t fspace = H5Dget_space(dset);
  int ndims = H5Sget_simple_extent_ndims(fspace);
  hsize_t *gdims = (hsize_t*) malloc(ndims*sizeof(hsize_t));
  H5Sget_simple_extent_dims(fspace, gdims, NULL);
  hsize_t dim = 1; // compute the size of a single sample
  for(int i=1; i<ndims; i++) {
    dim = dim*gdims[i];
  }
  H5DRMM.dset.sample.nel = dim;
  H5DRMM.dset.sample.dim = ndims-1;
  H5DRMM.dset.ns_glob = gdims[0];
  H5DRMM.dset.ns_cached=0;
  parallel_dist(gdims[0], H5DRMM.mpi.nproc, H5DRMM.mpi.rank, &H5DRMM.dset.ns_loc, &H5DRMM.dset.s_offset);
  H5DRMM.dset.sample.size = H5DRMM.dset.esize*H5DRMM.dset.sample.nel;
  H5DRMM.dset.size = H5DRMM.dset.sample.size*H5DRMM.dset.ns_loc;
  double t0 = MPI_Wtime();
  create_mmap_win(name);
  double t1 = MPI_Wtime() - t0;
  int rc = pthread_create(&H5DRMM.io.pthread, NULL, H5Dread_pthread_func, &H5DRMM);
  if (io_node() == H5DRMM.mpi.rank && debug_level() > 1)
    printf("Time for creating memory map files: %f seconds\n",  t1);
  free(gdims); 
  return dset;
};

/*
  Reading data from the file system and store it to local storage
*/
herr_t H5Dread_to_cache(hid_t dataset_id, hid_t mem_type_id,
			hid_t mem_space_id, hid_t file_space_id,
			hid_t xfer_plist_id, void * dat) {
  herr_t err = H5Dread(dataset_id, mem_type_id, mem_space_id, file_space_id, xfer_plist_id, dat);
  hsize_t bytes = get_buf_size(mem_space_id, mem_type_id);
  double t0 = MPI_Wtime();
  H5RPthreadWait();// notice that the first batch it will not wait
  get_samples_from_filespace(file_space_id, &H5DRMM.dset.batch, &H5DRMM.dset.contig_read);
  double t1 = MPI_Wtime() - t0;
  if (io_node()==H5DRMM.mpi.rank && debug_level() > 1) {
    printf("H5PthreadWait time: %f seconds\n", t1);
  }
  free(H5DRMM.mmap.tmp_buf);
  // copy the buffer
  H5DRMM.mmap.tmp_buf = malloc(bytes);
  t0 = MPI_Wtime();
  memcpy(H5DRMM.mmap.tmp_buf, dat, bytes);
  t1 = MPI_Wtime();
  if (io_node()==H5DRMM.mpi.rank && debug_level() > 1) {
    printf("H5Dread_cache memcpy: %f\n", t1-t0);
  }
  H5DRMM.dset.ns_cached += H5DRMM.dset.batch.size;
  pthread_mutex_lock(&H5DRMM.io.request_lock);
  H5DRMM.io.batch_cached = false;
  pthread_cond_signal(&H5DRMM.io.io_cond);
  pthread_mutex_unlock(&H5DRMM.io.request_lock);
  return err; 
}

/*
  Reading data directly from local storage. 
 */
herr_t H5Dread_cache(hid_t dataset_id, hid_t mem_type_id,
			  hid_t mem_space_id, hid_t file_space_id,
		       hid_t xfer_plist_id, void * dat) {
  if (H5DRMM.dset.ns_cached<H5DRMM.dset.ns_loc) {
    return H5Dread_to_cache(dataset_id, mem_type_id, mem_space_id, file_space_id, xfer_plist_id, dat);
  } else {
    return H5Dread_from_cache(dataset_id, mem_type_id, mem_space_id, file_space_id, xfer_plist_id, dat);
  }
}
herr_t H5Dread_from_cache(hid_t dataset_id, hid_t mem_type_id,
			  hid_t mem_space_id, hid_t file_space_id,
			  hid_t xfer_plist_id, void * dat) {
  if (io_node()==H5DRMM.mpi.rank && debug_level()>1) {
    printf("Reading data from cache (H5Dread_from_cache)\n");
  }
  bool contig = false;
  BATCH b;
  H5RPthreadWait();
  get_samples_from_filespace(file_space_id, &b, &contig);
  MPI_Win_fence(MPI_MODE_NOPUT | MPI_MODE_NOPRECEDE, H5DRMM.mpi.win);
  char *p_mem = (char *) dat;
  int batch_size = b.size;
  if (!contig) {
    for(int i=0; i< batch_size; i++) {
      int dest = b.list[i];
      int src = dest/H5DRMM.dset.ns_loc;
      MPI_Aint offset = (dest%H5DRMM.dset.ns_loc)*H5DRMM.dset.sample.nel;
      MPI_Get(&p_mem[i*H5DRMM.dset.sample.size],
	      H5DRMM.dset.sample.nel,
	      H5DRMM.dset.mpi_datatype, src,
	      offset, H5DRMM.dset.sample.nel,
	      H5DRMM.dset.mpi_datatype, H5DRMM.mpi.win);
    }
  } else {
    int dest = b.list[0];
    int src = dest/H5DRMM.dset.ns_loc;
    MPI_Aint offset = (dest%H5DRMM.dset.ns_loc)*H5DRMM.dset.sample.nel;
    MPI_Get(p_mem, H5DRMM.dset.sample.nel*batch_size,
	      H5DRMM.dset.mpi_datatype, src,
	      offset, H5DRMM.dset.sample.nel*batch_size,
	      H5DRMM.dset.mpi_datatype, H5DRMM.mpi.win);
  }
  MPI_Win_fence(MPI_MODE_NOSUCCEED, H5DRMM.mpi.win);
  return 0; 
}


herr_t H5DRMMF_remap() {
  H5RPthreadWait();
  hsize_t ss = (H5DRMM.dset.size/PAGESIZE+1)*PAGESIZE;
  if (strcmp("MEMORY", H5DRMM.ssd->path)!=0) {
    munmap(H5DRMM.mmap.buf, ss);
    close(H5DRMM.mmap.fd);
    H5DRMM.mmap.fd = open(H5DRMM.mmap.fname, O_RDWR);
    H5DRMM.mmap.buf = mmap(NULL, ss, PROT_READ, MAP_SHARED, H5DRMM.mmap.fd, 0);
    msync(H5DRMM.mmap.buf, ss, MS_SYNC);
  }
  return 0; 
}

herr_t H5Dclose_cache_read(hid_t dset) {
  H5RPthreadTerminate();
  MPI_Win_free(&H5DRMM.mpi.win);
  hsize_t ss = (H5DRMM.dset.size/PAGESIZE+1)*PAGESIZE;
  if (strcmp(H5DRMM.ssd->path, "MEMORY")!=0) {
    munmap(H5DRMM.mmap.buf, ss);
    free(H5DRMM.mmap.tmp_buf);
    close(H5DRMM.mmap.fd);
    remove(H5DRMM.mmap.fname);
  } else {
    free(H5DRMM.mmap.buf);
  }
  return H5Dclose(dset);
}
/*
  Terminate join the Pthread
 */
void H5RPthreadTerminate() {
  H5RPthreadWait();
  pthread_mutex_lock(&H5DRMM.io.request_lock);
  H5DRMM.io.batch_cached=true;
  H5DRMM.io.dset_cached=true;
  pthread_cond_signal(&H5DRMM.io.io_cond);
  pthread_mutex_unlock(&H5DRMM.io.request_lock);
  pthread_join(H5DRMM.io.pthread, NULL);
}

/*
  Waiting for Pthread to finish all the I/O tasks
 */
void H5RPthreadWait() {
  pthread_mutex_lock(&H5DRMM.io.request_lock);
  while(!H5DRMM.io.batch_cached) {
    pthread_cond_signal(&H5DRMM.io.io_cond);
    pthread_cond_wait(&H5DRMM.io.master_cond, &H5DRMM.io.request_lock);
  }
  pthread_mutex_unlock(&H5DRMM.io.request_lock);
}

