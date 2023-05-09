/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (c) 2023, UChicago Argonne, LLC.                                *
 * All Rights Reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5 Cache VOL connector.  The full copyright notice *
 * terms governing use, modification, and redistribution, is contained in    *
 * the LICENSE file, which can be found at the root of the source code       *
 * distribution tree.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#include "H5LS.h"
#include "cache_utils.h"
#include <fcntl.h>
#include <libgen.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>

/*-------------------------------------------------------------------------
 * Function:    H5Ssel_gather_write
 *
 * Purpose:     Copy the data buffer into local storage.
 *
 * Return:      NULL
 *
 *-------------------------------------------------------------------------
 */
static herr_t H5Ssel_gather_write(hid_t space, hid_t tid, const void *buf,
                                  int fd, hsize_t offset) {
  unsigned flags = H5S_SEL_ITER_GET_SEQ_LIST_SORTED;
  size_t elmt_size = H5Tget_size(tid);
  hid_t iter = H5Ssel_iter_create(space, elmt_size, flags);
  size_t maxseq = H5Sget_select_npoints(space);
  size_t maxbytes = maxseq * elmt_size;
  size_t nseq, nbytes;
  size_t *len = (size_t *)malloc(maxseq * sizeof(size_t));
  hsize_t *off = (hsize_t *)malloc(maxseq * sizeof(hsize_t));
  H5Ssel_iter_get_seq_list(iter, maxseq, maxbytes, &nseq, &nbytes, off, len);
  hsize_t off_contig = 0;
  char *p = (char *)buf;
  int i;
  for (i = 0; i < nseq; i++) {
    int err = pwrite(fd, &p[off[i]], len[i], offset + off_contig);
    off_contig += len[i];
  }
#ifdef __APPLE__
  fcntl(fd, F_NOCACHE, 1);
#else
  fsync(fd);
#endif
  return 0;
}

static herr_t H5LS_SSD_create_write_mmap(MMAP *mm, hsize_t size) {
  char dname[255];
  strcpy(dname, mm->fname);
  mkdirRecursive(dirname(dname), 0755); // dirname will change dname in linux.
                                        // therefore, we make copy first.
  struct stat info;
  mm->fd = open(mm->fname, O_RDWR | O_CREAT | O_TRUNC, 0644);
  return 0;
}

/* remove data from write space */
static herr_t H5LS_SSD_remove_write_mmap(MMAP *mm, hsize_t size) {
  if (access(mm->fname, F_OK) == 0)
    remove(mm->fname);
  return 0;
}

/* write data from memspace to mmap files */
static void *H5LS_SSD_write_buffer_to_mmap(hid_t mem_space_id,
                                           hid_t mem_type_id, const void *buf,
                                           hsize_t size, MMAP *mm) {
  H5Ssel_gather_write(mem_space_id, mem_type_id, buf, mm->fd, mm->offset);
  void *p = mmap(NULL, size, PROT_READ, MAP_SHARED, mm->fd, mm->offset);
  msync(p, size, MS_SYNC);
  return p;
}

/* create read mmap buffer, files */
static herr_t H5LS_SSD_create_read_mmap(MMAP *mm, hsize_t size) {
  char tmp[255];
  strcpy(tmp, mm->fname);
  mkdirRecursive(dirname(tmp), 0755);
  int fh = open(mm->fname, O_RDWR | O_CREAT | O_TRUNC,
                S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  char a = 'A';
  pwrite(fh, &a, 1, size);
  close(fh);
  mm->fd = open(mm->fname, O_RDWR);
  mm->buf = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_NORESERVE,
                 mm->fd, 0);
  return 0;
};

/* clean up read mmap buffer, files */
static herr_t H5LS_SSD_remove_read_mmap(MMAP *mm, hsize_t size) {
  herr_t ret;
  munmap(mm->buf, size);
  close(mm->fd);
  if (access(mm->fname, F_OK) == 0)
    remove(mm->fname);
  return 0;
};

const H5LS_mmap_class_t H5LS_SSD_mmap_ext_g = {
    "SSD",
    H5LS_SSD_create_write_mmap,
    H5LS_SSD_remove_write_mmap,
    H5LS_SSD_write_buffer_to_mmap,
    H5LS_SSD_create_read_mmap,
    H5LS_SSD_remove_read_mmap,
    rmdirRecursive,
};
