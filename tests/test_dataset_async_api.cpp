/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (c) 2023, UChicago Argonne, LLC.                                *
 * All Rights Reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5 Cache VOL connector.  The full copyright notice *
 * terms governing use, modification, and redistribution, is contained in    *
 * the LICENSE file, which can be found at the root of the source code       *
 * distribution tree.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#include "cache_new_h5api.h"
#include "hdf5.h"
#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"
#include <fcntl.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
void int2char(int a, char str[255]) { sprintf(str, "%d", a); }

int main(int argc, char **argv) {
  // Assuming that the dataset is a two dimensional array of 8x5 dimension;
  size_t d1 = 2048;
  size_t d2 = 2048;
  hsize_t ldims[2] = {d1, d2};
  hsize_t oned = d1 * d2;
  MPI_Comm comm = MPI_COMM_WORLD;
  MPI_Info info = MPI_INFO_NULL;
  int rank, nproc, provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  MPI_Comm_size(comm, &nproc);
  MPI_Comm_rank(comm, &rank);
  hsize_t gdims[2] = {d1 * nproc, d2};
  if (rank == 0) {
    printf("****HDF5 Testing Dataset*****\n");
    printf("=============================================\n");
    printf(" Buf dim: %llu x %llu\n", ldims[0], ldims[1]);
    printf("   nproc: %d\n", nproc);
    printf("=============================================\n");
  }
  hsize_t offset[2] = {0, 0};
  // setup file access property list for mpio
  hid_t plist_id = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_fapl_mpio(plist_id, comm, info);
  bool p = true;
  char f[255];
  strcpy(f, "parallel_file.h5");
  // create memory space
  hid_t memspace = H5Screate_simple(2, ldims, NULL);
  // define local data
  int *data = (int *)malloc(ldims[0] * ldims[1] * sizeof(int));
  // set up dataset access property list
  for (int i = 0; i < ldims[0] * ldims[1]; i++)
    data[i] = rank + 1;
  hid_t dxf_id = H5Pcreate(H5P_DATASET_XFER);
  herr_t ret = H5Pset_dxpl_mpio(dxf_id, H5FD_MPIO_COLLECTIVE);

  hid_t filespace = H5Screate_simple(2, gdims, NULL);
  hid_t dt = H5Tcopy(H5T_NATIVE_INT);

  hsize_t count[2] = {1, 1};
  if (rank == 0)
    printf("Creating file %s \n", f);
  hid_t file_id = H5Fcreate(f, H5F_ACC_TRUNC, H5P_DEFAULT, plist_id);
  int niter = 1;
  offset[0] = rank * ldims[0];
  H5Sselect_hyperslab(filespace, H5S_SELECT_SET, offset, NULL, ldims, count);
  char str[255];
  hid_t es_id = H5EScreate();
  for (int it = 0; it < niter; it++) {
    int2char(it, str);
    if (rank == 0)
      printf("Creating group %s \n", str);
    hid_t grp_id = H5Gcreate_async(file_id, str, H5P_DEFAULT, H5P_DEFAULT,
                                   H5P_DEFAULT, es_id);
    if (rank == 0)
      printf("Creating dataset %s \n", "dset_test");
    hid_t dset = H5Dcreate_async(grp_id, "dset_test", H5T_NATIVE_INT, filespace,
                                 H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT, es_id);
    hid_t dset2 =
        H5Dcreate_async(grp_id, "dset_test2", H5T_NATIVE_INT, filespace,
                        H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT, es_id);
    // hid_t dset2 = H5Dcreate(grp_id, "dset_test2", H5T_NATIVE_INT, filespace,
    // H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    if (rank == 0)
      printf("Writing dataset %s \n", "dset_test");
    // H5Fcache_async_op_pause(file_id);
    hid_t status = H5Dwrite_async(dset, H5T_NATIVE_INT, memspace, filespace,
                                  dxf_id, data, es_id); // write memory to file
    hid_t status2 = H5Dwrite_async(dset2, H5T_NATIVE_INT, memspace, filespace,
                                   dxf_id, data, es_id); // write memory to file
    // H5Fcache_async_op_start(file_id);
    // hid_t status2 = H5Dwrite(dset2, H5T_NATIVE_INT, memspace, filespace,
    // dxf_id, data); // write memory to file
    if (rank == 0)
      printf("Closing dataset %s \n", "dset_test");
    H5Dclose_async(dset, es_id);
    H5Dclose_async(dset2, es_id);

    if (rank == 0)
      printf("Closing group %s \n", str);
    // H5Dclose(dset2);
    H5Gclose_async(grp_id, es_id);
  }
  size_t num_inprogress;
  hbool_t error_occured;
  H5ESwait(es_id, UINT_MAX, &num_inprogress, &error_occured);
  H5ESclose(es_id);
  H5Fflush(file_id, H5F_SCOPE_LOCAL);
  if (rank == 0)
    printf("Closing file %s \n====================\n\n", f);

  H5Fclose(file_id);
  H5Pclose(dxf_id);
  H5Sclose(filespace);
  H5Sclose(memspace);
  MPI_Finalize();
  return 0;
}
