#include <stdio.h>
// Memory map
// POSIX I/O
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <fcntl.h>
#include "string.h"
#include <unistd.h>
#include "H5LS.h"

#include <assert.h>

#include <cuda.h>
#include <cuda_runtime_api.h>
#define CUDA_RUNTIME_API_CALL(apiFuncCall)                               \
{                                                                        \
  cudaError_t _status = apiFuncCall;                                     \
  if (_status != cudaSuccess) {                                          \
    fprintf(stderr, "%s:%d: error: function %s failed with error %s.\n", \
      __FILE__, __LINE__, #apiFuncCall, cudaGetErrorString(_status));    \
    exit(-1);                                                            \
  }                                                                      \
}


/*-------------------------------------------------------------------------
 * Function:    H5Ssel_gather_copy
 *
 * Purpose:     Copy the data buffer into gpu.
 *
 * Return:      NULL
 *
 *-------------------------------------------------------------------------
 */
static herr_t H5Ssel_gather_copy(hid_t space, hid_t tid, const void *buf, void *mbuf, hsize_t offset) {
  // assert(false);
  unsigned flags = H5S_SEL_ITER_GET_SEQ_LIST_SORTED;
  size_t elmt_size =  H5Tget_size(tid);
  hid_t iter = H5Ssel_iter_create(space, elmt_size, flags);
  size_t maxseq = H5Sget_select_npoints(space);
  size_t maxbytes = maxseq*elmt_size;
  size_t nseq, nbytes;
  size_t *len = (size_t*)malloc(maxseq*sizeof(size_t));
  hsize_t *off = (hsize_t*)malloc(maxseq*sizeof(hsize_t));
  H5Ssel_iter_get_seq_list(iter, maxseq, maxbytes, &nseq, &nbytes, off, len);
  hsize_t off_contig=0;
  char *p = (char*) buf;
  char *mp = (char*) mbuf;
  // cudaStream_t stream0;
  // cudaStreamCreate(&stream0);

  for(int i=0; i<nseq; i++) {
    // memcpy(&mp[offset+off_contig], &p[off[i]], len[i]);
    CUDA_RUNTIME_API_CALL( cudaMemcpy(&mp[offset+off_contig], &p[off[i]], len[i], cudaMemcpyDeviceToHost ) );
    // CUDA_RUNTIME_API_CALL( cudaMemcpyAsync(&mp[offset+off_contig], &p[off[i]], len[i], cudaMemcpyDeviceToHost, stream0 ) );
    // CUDA_RUNTIME_API_CALL( cudaMemPrefetchAsync(&mp[offset+off_contig], len[i], 1, 0) );
    off_contig += len[i];
  }
  return 0;
} /* end  H5Ssel_gather_copy() */

static
void *H5LS_GPU_write_buffer_to_mmap(hid_t mem_space_id, hid_t mem_type_id, const void *buf, hsize_t size, MMAP *mm) {
  H5Ssel_gather_copy(mem_space_id, mem_type_id, buf, mm->buf, mm->offset);
  void *p=mm->buf + mm->offset;
  return p;
}

static herr_t H5LS_GPU_create_write_mmap(MMAP *mm, hsize_t size)
{
  // mm->buf = malloc(size);
  // int gpu_id = -1;
  // CUDA_RUNTIME_API_CALL( cudaGetDevice ( &gpu_id ) );

  // int device_id = 0, result = 0;
  // CUDA_RUNTIME_API_CALL(cudaSetDevice(device_id));
  // CUDA_RUNTIME_API_CALL(cudaDeviceGetAttribute (&result, cudaDevAttrConcurrentManagedAccess, device_id));


//  int devCount;
//  CUDA_RUNTIME_API_CALL(cudaGetDeviceCount(&devCount));
//  // fprintf(stderr, "CUDA Device Query...\n");
//  // fprintf(stderr, "There are %d CUDA devices.\n", devCount);
//  // printf("mpi_rank: %d\n", MY_RANK);
//  // fflush(stderr);
//  int local_rank;
//  char *str;
//  int gpu_id = -1;
//  
//  if ((str = getenv ("OMPI_COMM_WORLD_LOCAL_RANK")) != NULL) {
//    local_rank = atoi(getenv("OMPI_COMM_WORLD_LOCAL_RANK"));
//    gpu_id = local_rank;
//    // fprintf(stderr, "mpi_rank: %d\n", local_rank);
//  }
//  else {
//    printf("OMPI_COMM_WORLD_LOCAL_RANK is NULL\n");
//    gpu_id = 0;
//  }
//  
//  gpu_id %= devCount; // there are only devCount
//  CUDA_RUNTIME_API_CALL(cudaSetDevice(gpu_id));

  CUDA_RUNTIME_API_CALL( cudaHostAlloc(&mm->buf, size, cudaHostAllocDefault) );
  // CUDA_RUNTIME_API_CALL( cudaMallocManaged(&mm->buf, size, cudaMemAttachGlobal) );

  // CUDA_RUNTIME_API_CALL( cudaMemAdvise(mm->buf, size, cudaMemAdviseSetPreferredLocation, device_id) );
  // CUDA_RUNTIME_API_CALL( cudaMemAdvise(mm->buf, size, cudaMemAdviseSetAccessedBy, device_id) );
  // CUDA_RUNTIME_API_CALL( cudaMemAdvise(mm->buf, size, cudaMemAdviseSetAccessedBy, gpu_id) );
  // CUDA_RUNTIME_API_CALL( cudaMemAdvise(&mm->buf, size, cudaMemAdviseSetPreferredLocation, gpu_id) );

  // CUDA_RUNTIME_API_CALL( cudaMalloc(&mm->buf, size) );
  return 0;
};

static herr_t H5LS_GPU_create_read_mmap(MMAP *mm, hsize_t size){
  // mm->buf = malloc(size);
  // cudaMallocManaged(&mm->buf, size, cudaMemAttachGlobal);
  // cudaMalloc(&mm->buf, size);
  return 0;
}

static herr_t H5LS_GPU_remove_write_mmap(MMAP *mm, hsize_t size) {
  // free(mm->buf);
  // mm->buf = NULL;
  // CUDA_RUNTIME_API_CALL( cudaFree(mm->buf) );
  CUDA_RUNTIME_API_CALL( cudaFreeHost(mm->buf) );
  return 0;
}

static herr_t H5LS_GPU_remove_read_mmap(MMAP *mm, hsize_t size) {
  // free(mm->buf);
  // mm->buf = NULL;
  CUDA_RUNTIME_API_CALL( cudaFree(mm->buf) );
  return 0;
}

static herr_t removeFolderFake(const char *path) {
  return 0;
};

const H5LS_mmap_class_t H5LS_GPU_mmap_ext_g = {
  "GPU",
  H5LS_GPU_create_write_mmap,
  H5LS_GPU_remove_write_mmap,
  H5LS_GPU_write_buffer_to_mmap,
  H5LS_GPU_create_read_mmap,
  H5LS_GPU_remove_read_mmap,
  removeFolderFake,
};
