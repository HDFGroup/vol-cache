/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (c) 2023, UChicago Argonne, LLC.                                *
 * All Rights Reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5 Cache VOL connector.  The full copyright notice *
 * terms governing use, modification, and redistribution, is contained in    *
 * the LICENSE file, which can be found at the root of the source code       *
 * distribution tree.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

//
// This test example is for testing the multiple dataset API.
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
  size_t d1 = 1024;
  size_t d2 = 1024;
  hsize_t ldims[2] = {d1, d2};
  hsize_t oned = d1 * d2;
  MPI_Comm comm = MPI_COMM_WORLD;
  MPI_Info info = MPI_INFO_NULL;
  int rank, nproc, provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  MPI_Comm_size(comm, &nproc);
  MPI_Comm_rank(comm, &rank);
  hsize_t gdims[2] = {d1 * nproc, d2};
  bool collective = false;
  bool multi = false;
  for (int i = 1; i < argc; i++)
    if (strcmp(argv[i], "--collective") == 0) {
      collective = true;
    } else if (strcmp(argv[i], "--multi") == 0) {
      multi = true;
    }

  if (rank == 0) {
    printf("****HDF5 Testing Dataset*****\n");
    printf("=============================================\n");
    printf(" Buf dim: %lu x %lu\n", ldims[0], ldims[1]);
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
  const void *buf[2];
  buf[0] = malloc(ldims[0] * ldims[1] * sizeof(int));
  buf[1] = malloc(ldims[0] * ldims[1] * sizeof(int));
  int *data = (int *)buf[0];
  int *data2 = (int *)buf[1];

  // set up dataset access property list
  for (int i = 0; i < ldims[0] * ldims[1]; i++) {
    data[i] = rank + 1;
    data2[i] = rank + 2;
  }
  hid_t dxf_id = H5Pcreate(H5P_DATASET_XFER);
  if (collective) {
    herr_t ret = H5Pset_dxpl_mpio(dxf_id, H5FD_MPIO_COLLECTIVE);
    if (rank == 0)
      printf("Collective write\n");
    else
      printf("Independent write\n");
  }

  hid_t filespace = H5Screate_simple(2, gdims, NULL);

  hsize_t count[2] = {1, 1};
  if (rank == 0)
    printf("Creating file %s \n", f);
  hid_t file_id = H5Fcreate(f, H5F_ACC_TRUNC, H5P_DEFAULT, plist_id);
  int niter = 1;
  offset[0] = rank * ldims[0];
  H5Sselect_hyperslab(filespace, H5S_SELECT_SET, offset, NULL, ldims, count);
  char str[255];
  for (int it = 0; it < niter; it++) {
    int2char(it, str);
    if (rank == 0)
      printf("Creating group %s \n", str);
    hid_t grp_id =
        H5Gcreate(file_id, str, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    if (rank == 0)
      printf("Creating dataset %s \n", "dset_test, and dset_test2");
    hid_t dset = H5Dcreate(grp_id, "dset_test", H5T_NATIVE_INT, filespace,
                           H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    hid_t dset2 = H5Dcreate(grp_id, "dset_test2", H5T_NATIVE_INT, filespace,
                            H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    if (rank == 0)
      printf(" writing dataset %s \n", "dset_test2");
    hid_t dset_mult[2] = {dset, dset2};
    hid_t memtype_mult[2] = {H5T_NATIVE_INT, H5T_NATIVE_INT};
    hid_t filespace_mult[2] = {filespace, filespace};
    hid_t memspace_mult[2] = {memspace, memspace};
    if (multi) {
#if H5_VERSION_GE(1, 13, 3)
      printf(" Using multi dataset API\n");
      hid_t status = H5Dwrite_multi(2, dset_mult, memtype_mult, memspace_mult,
                                    filespace_mult, dxf_id, buf);
#else
      printf(" WARNNING: Current HDF5 version does not support H5Dwrite_multi, "
             "using H5Dwrite instead\n");
      hid_t status = H5Dwrite(dset_mult[0], memtype_mult[0], memspace_mult[0],
                              filespace_mult[0], dxf_id,
                              buf[0]); // write memory to file
      status = H5Dwrite(dset_mult[1], memtype_mult[1], memspace_mult[1],
                        filespace_mult[1], dxf_id,
                        buf[1]); // write memory to file
#endif
    } else {
      hid_t status = H5Dwrite(dset_mult[0], memtype_mult[0], memspace_mult[0],
                              filespace_mult[0], dxf_id,
                              buf[0]); // write memory to file
      status = H5Dwrite(dset_mult[1], memtype_mult[1], memspace_mult[1],
                        filespace_mult[1], dxf_id,
                        buf[1]); // write memory to file
    }
    if (rank == 0)
      printf("Closing dataset %s \n", "dset_test");
    H5Dclose(dset);
    if (rank == 0)
      printf("Closing group %s \n", str);
    H5Dclose(dset2);
    H5Gclose(grp_id);
  }
  if (rank == 0)
    printf("Closing file %s\n", f);
  H5Fflush(file_id, H5F_SCOPE_LOCAL);
  H5Fclose(file_id);
  H5Pclose(dxf_id);
  H5Sclose(filespace);
  H5Sclose(memspace);
  MPI_Finalize();
  return 0;
}