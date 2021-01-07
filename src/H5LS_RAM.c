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

/*-------------------------------------------------------------------------
 * Function:    H5Ssel_gather_copy
 *
 * Purpose:     Copy the data buffer into memory. 
 *
 * Return:      NULL
 *
 *-------------------------------------------------------------------------
 */
static herr_t H5Ssel_gather_copy(hid_t space, hid_t tid, const void *buf, void *mbuf, hsize_t offset) {
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
  for(int i=0; i<nseq; i++) {
    memcpy(&mp[offset+off_contig], &p[off[i]], len[i]); 
    off_contig += len[i];
  }
  return 0;
} /* end  H5Ssel_gather_copy() */

static 
void *H5LS_RAM_write_buffer_to_mmap(hid_t mem_space_id, hid_t mem_type_id, const void *buf, hsize_t size, MMAP *mm) {
  H5Ssel_gather_copy(mem_space_id, mem_type_id, buf, mm->buf, mm->offset);
  void *p=mm->buf + mm->offset;
  return p; 
}

static herr_t H5LS_RAM_create_write_mmap(MMAP *mm, hsize_t size)
{
  mm->buf = malloc(size);
  return 0; 
};

static herr_t H5LS_RAM_create_read_mmap(MMAP *mm, hsize_t size){
  mm->buf = malloc(size);
  return 0; 
}

static herr_t H5LS_RAM_remove_write_mmap(MMAP *mm, hsize_t size) {
  free(mm->buf);
  mm->buf = NULL; 
  return 0; 
}

static herr_t H5LS_RAM_remove_read_mmap(MMAP *mm, hsize_t size) {
  free(mm->buf);
  mm->buf = NULL;
  return 0;
}

static herr_t removeFolderFake(const char *path) {
  return 0; 
};

const H5LS_mmap_class_t H5LS_RAM_mmap_ext_g = {
  "MEMORY",
  H5LS_RAM_create_write_mmap,
  H5LS_RAM_remove_write_mmap,
  H5LS_RAM_write_buffer_to_mmap,
  H5LS_RAM_create_read_mmap,
  H5LS_RAM_remove_read_mmap,
  removeFolderFake, 
};
