/*
  This is the header files for node local storage incorporated HDF5
 */
#ifndef UTILS_H_
#define UTILS_H_
#include "hdf5.h"
#include "mpi.h"
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <limits.h>
#include "H5LS.h"
#ifndef MAXDIM
#define MAXDIM 32
#endif
// The meta data for I/O thread to perform parallel write

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
  void mkdirRecursive(const char *path, mode_t mode);
  herr_t rmdirRecursive(const char *path); 
#ifdef __cplusplus
}
#endif
#endif //UTILS_H_
