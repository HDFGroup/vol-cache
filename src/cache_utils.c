/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (c) 2023, UChicago Argonne, LLC.                                *
 * All Rights Reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5 Cache VOL connector.  The full copyright notice *
 * terms governing use, modification, and redistribution, is contained in    *
 * the LICENSE file, which can be found at the root of the source code       *
 * distribution tree.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#include "mpi.h"
#include "stdlib.h"
#include "string.h"
#include "unistd.h"
#include <stdio.h>
// POSIX I/O
#include "cache_utils.h"
#include "hdf5.h"
#include "sys/stat.h"
#include <dirent.h>
#include <sys/mman.h>
#include <sys/statvfs.h>
#include <sys/types.h>

// Debug
#include "debug.h"
/*
   Global variables to define information related to the local storage
*/
#define MAXDIM 32
#define PAGESIZE sysconf(_SC_PAGE_SIZE)

void int2char(int a, char str[255]) { sprintf(str, "%d", a); }

/*
  Purpose: get the size of the buffer from the memory space and type id
  Output: the size of the memory space in bytes.
 */
hsize_t get_buf_size(hid_t mspace, hid_t tid) {
  hsize_t nelement = H5Sget_select_npoints(mspace);
  hsize_t s = H5Tget_size(tid);
  return s * nelement;
}

/*
  Thread function for performing H5Dwrite. This function will create
  a memory mapped buffer to the file that is on the local storage which
  contains the data to be written to the file system.

  On Theta, the memory mapped buffer currently does not work with H5Dwrite, we
  instead allocate a buffer directly to the memory.
*/

void parallel_dist(size_t gdim, int nproc, int rank, size_t *ldim,
                   size_t *start) {
  *ldim = gdim / nproc;
  *start = *ldim * rank;
  if (rank < gdim % nproc) {
    *ldim += 1;
    *start += rank;
  } else {
    *start += gdim % nproc;
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
  int i;
  for (i = 1; i < ndims; i++) {
    offset[i] = 0;
    sample[i] = gdims[i];
    count[i] = 1;
  }
  H5Sselect_hyperslab(*fspace, H5S_SELECT_SET, offset, NULL, sample, count);
  for (i = 1; i < nsample; i++) {
    offset[0] = samples[i];
    H5Sselect_hyperslab(*fspace, H5S_SELECT_OR, offset, NULL, sample, count);
  }
}
/*
  Get the indices of the samples that have been selected from filespace, and
  check whether it is contiguous or not.
*/
void get_samples_from_filespace(hid_t fspace, BATCH *samples, bool *contig) {
  hssize_t numblocks = H5Sget_select_hyper_nblocks(fspace);
  hsize_t gdims[MAXDIM];
  int ndims = H5Sget_simple_extent_dims(fspace, gdims, NULL);
  hsize_t *block_buf =
      (hsize_t *)malloc(numblocks * 2 * ndims * sizeof(hsize_t));
  H5Sget_select_hyper_blocklist(fspace, 0, numblocks, block_buf);
  samples->size = 0;
  int i, j;
  for (i = 0; i < numblocks; i++) {
    int start = block_buf[2 * i * ndims];
    int end = block_buf[2 * i * ndims + ndims];
    for (j = start; j < end + 1; j++) {
      samples->size = samples->size + 1;
    }
  }
  samples->list = (int *)malloc(sizeof(int) * samples->size);
  int n = 0;
  for (i = 0; i < numblocks; i++) {
    int start = block_buf[2 * i * ndims];
    int end = block_buf[2 * i * ndims + ndims];
    for (j = start; j < end + 1; j++) {
      samples->list[n] = j;
      n = n + 1;
    }
  }
  *contig = H5Sis_regular_hyperslab(fspace);
  free(block_buf);
}

/*
   Create directory recursively by providing a path.
*/
void mkdirRecursive(const char *path, mode_t mode) {
  char opath[PATH_MAX];
  char *p;
  size_t len;

  strncpy(opath, path, sizeof(opath));
  opath[sizeof(opath) - 1] = '\0';
  len = strlen(opath);
  if (len == 0)
    return;
  else if (opath[len - 1] == '/')
    opath[len - 1] = '\0';
  for (p = opath; *p; p++)
    if (*p == '/') {
      *p = '\0';
      if (access(opath, F_OK))
        mkdir(opath, mode);
      *p = '/';
    }
  if (access(opath, F_OK)) /* if path is not terminated with / */
    mkdir(opath, mode);
}

/*
   Create directory recursively (removing the folder and everything below it)
*/
herr_t rmdirRecursive(const char *path) {
  herr_t ret;
#ifndef NDEBUG
  LOG_DEBUG(-1, "remove folder: %s\n", path);
#endif
  DIR *theFolder = opendir(path);
  struct dirent *next_file;
  char filepath[287];
  while ((next_file = readdir(theFolder)) != NULL) {
    sprintf(filepath, "%s/%s", path, next_file->d_name);
    LOG_DEBUG(-1, "remove_cache filepath: %s", filepath);
    ret = remove(filepath);
  }
  ret = closedir(theFolder);
  ret = rmdir(path);
  return ret;
}
