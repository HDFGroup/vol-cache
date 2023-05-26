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
  size_t d1 = 2;
  size_t d2 = 2;
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
  for (int i = 1; i < argc; i++)
    if (strcmp(argv[i], "--collective") == 0) {
      collective = true;
    }

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
  int *data2 = (int *)malloc(ldims[0] * ldims[1] * sizeof(int));

  // set up dataset access property list
  for (int i = 0; i < ldims[0] * ldims[1]; i++) {
    data[i] = rank + 1;
    data2[i] = rank+2; 
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
      printf("Creating dataset %s \n", "dset_test");
    hid_t dset = H5Dcreate(grp_id, "dset_test", H5T_NATIVE_INT, filespace,
                           H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    hid_t dset2 = H5Dcreate(grp_id, "dset_test2", H5T_NATIVE_INT, filespace,
                            H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    if (rank == 0)
      printf("Writing dataset %s \n", "dset_test");
    hid_t dset_mult[2] = {dset, dset2}; 
    hid_t memtype_mult[2] = {H5T_NATIVE_INT, H5T_NATIVE_INT}; 
    hid_t filespace_mult[2] = {filespace, filespace}; 
    hid_t memspace_mult[2] = {memspace, memspace}; 
    const void *buf[2];
    buf[1] = (void *) data;
    buf[2] = (void *) data2;

    hid_t status = H5Dwrite_multi(2, dset_mult, memtype_mult, memspace_mult,
                              filespace_mult, dxf_id, buf);

    //hid_t status = H5Dwrite(dset, H5T_NATIVE_INT, memspace, filespace, dxf_id,
    //                        data); // write memory to file
    //hid_t status2 = H5Dwrite(dset2, H5T_NATIVE_INT, memspace, filespace, dxf_id,
    //                         data); // write memory to file
    if (rank == 0)
      printf("Closing dataset %s \n", "dset_test");
    H5Dclose(dset);
    if (rank == 0)
      printf("Closing group %s \n", str);
    H5Dclose(dset2);
    H5Gclose(grp_id);
  }
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

/*
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
  size_t d1 = 2;
  size_t d2 = 2;
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
  for (int i = 1; i < argc; i++)
    if (strcmp(argv[i], "--collective") == 0) {
      collective = true;
    }

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
  // define local data
  int *data = (int *)malloc(ldims[0] * ldims[1] * sizeof(int));
  int *data2 = (int *)malloc(ldims[0] * ldims[1] * sizeof(int));

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

  hid_t dt = H5Tcopy(H5T_NATIVE_INT);

  hsize_t count[2] = {1, 1};
  if (rank == 0)
    printf("Creating file %s \n", f);
  hid_t file_id = H5Fcreate(f, H5F_ACC_TRUNC, H5P_DEFAULT, plist_id);
  int niter = 1;
  offset[0] = rank * ldims[0];
  hid_t mem_space_id[2]; 
  hid_t file_space_id[2]; 
  file_space_id[0] = H5Screate_simple(2, gdims, NULL);
  file_space_id[1] = H5Screate_simple(2, gdims, NULL);
  H5Sselect_hyperslab( file_space_id[0], H5S_SELECT_SET, offset, NULL, ldims, count);
  H5Sselect_hyperslab( file_space_id[1], H5S_SELECT_SET, offset, NULL, ldims, count);
  char str[255];
  for (int it = 0; it < niter; it++) {
    int2char(it, str);
    if (rank == 0)
      printf("Creating group %s \n", str);
    hid_t grp_id =
        H5Gcreate(file_id, str, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    if (rank == 0)
      printf("Creating dataset %s \n", "dset_test");


    hid_t d1  = H5Dcreate(grp_id, "dset_test", H5T_NATIVE_INT, file_space_id[0],
                        H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    hid_t d2 = H5Dcreate(grp_id, "dset_test2", H5T_NATIVE_INT, file_space_id[1],
                        H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    hid_t dset[2] = {d1, d2}; 
    if (rank == 0)
      printf("Writing dataset %s \n", "dset_test");

    hid_t mem_type_id[2] = {H5T_NATIVE_INT, H5T_NATIVE_INT};
    const void *buf[2];
    buf[1] = data;
    buf[2] = data2;

    mem_space_id[0] = H5Screate_simple(2, ldims, NULL);
    mem_space_id[1] = H5Screate_simple(2, ldims, NULL);

//#if H5_VERSION_GE(1, 13, 3)
    //   hid_t status = H5Dwrite_multi(2, dset, mem_type_id, mem_space_id,
    //                              file_space_id, dxf_id, buf);
//#else
    printf(
        "This test only work for HDF5 version equal to 1.13.3 or greater. \n");
    printf("Your version does not support H5Dwrite_multi. I will do H5Dwrite "
           "instead\n");
    hid_t status = H5Dwrite(d1,  H5T_NATIVE_INT, mem_space_id[0],
                            file_space_id[0], dxf_id, buf[0]);
    status = H5Dwrite(d2,  H5T_NATIVE_INT, mem_space_id[1],
                      file_space_id[1], dxf_id, buf[1]);
//#endif
    if (rank == 0)
      printf("Closing dataset %s \n", "dset_test");
    if (rank == 0)
      printf("Closing group %s \n", str);
    for (int i = 0; i < 2; i++) {
      H5Dclose(dset[i]);
      H5Sclose(mem_space_id[i]);
      H5Sclose(file_space_id[i]);
    }
    H5Gclose(grp_id);
  }
  H5Fflush(file_id, H5F_SCOPE_LOCAL);
  if (rank == 0)
    printf("Closing file %s \n====================\n\n", f);

  H5Fclose(file_id);
  H5Pclose(dxf_id);
//  H5Sclose(filespace);
//  H5Sclose(memspace);
  MPI_Finalize();
  return 0;
}
*/