/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (c) 2023, UChicago Argonne, LLC.                                *
 * All Rights Reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5 Cache VOL connector.  The full copyright notice *
 * terms governing use, modification, and redistribution, is contained in    *
 * the LICENSE file, which can be found at the root of the source code       *
 * distribution tree.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

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
    printf("*** Testing open up a group and close it ***\n");
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
  hid_t dxf_id = H5Pcreate(H5P_DATASET_XFER);
  H5Pset_dxpl_mpio(dxf_id, H5FD_MPIO_COLLECTIVE);

  hid_t filespace = H5Screate_simple(2, gdims, NULL);
  hid_t dt = H5Tcopy(H5T_NATIVE_INT);

  hsize_t count[2] = {1, 1};
  hid_t file_id = H5Fcreate(f, H5F_ACC_TRUNC, H5P_DEFAULT, plist_id);
  hid_t grp_id =
      H5Gcreate(file_id, "group_test", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
  H5Gclose(grp_id);
  H5Fflush(file_id, H5F_SCOPE_LOCAL);
  H5Fclose(file_id);
  MPI_Finalize();
  return 0;
}
