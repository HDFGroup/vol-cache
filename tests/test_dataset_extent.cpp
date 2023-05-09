/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (c) 2023, UChicago Argonne, LLC.                                *
 * All Rights Reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5 Cache VOL connector.  The full copyright notice *
 * terms governing use, modification, and redistribution, is contained in    *
 * the LICENSE file, which can be found at the root of the source code       *
 * distribution tree.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 *   This example shows how to work with extendible dataset.
 *   In the current version of the library dataset MUST be
 *   chunked.
 *
 */

#include "hdf5.h"
#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#define H5FILE_NAME "SDSextendible.h5"
#define DATASETNAME "ExtendibleArray"
#define RANK 2

int main(int argc, char **argv) {
  hid_t file; /* handles */
  hid_t dataspace, dataset;
  hid_t filespace;
  hid_t cparms;
  hsize_t dims[2] = {3, 3}; /*
                             * dataset dimensions
                             * at the creation time
                             */
  hsize_t maxdims[2] = {H5S_UNLIMITED, H5S_UNLIMITED};
  hsize_t chunk_dims[2] = {3, 3};
  hsize_t offset[2];
  herr_t status;
  int niter = 4;

  int fillvalue = 0;

  MPI_Comm comm = MPI_COMM_WORLD;
  MPI_Info info = MPI_INFO_NULL;
  int rank, nproc, provided;
  bool collective = false;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  MPI_Comm_size(comm, &nproc);
  MPI_Comm_rank(comm, &rank);
  printf("I am rank %d of %d\n", rank, nproc);
  for (int i = 0; i < argc; i++) {
    if (strcmp(argv[i], "--collective") == 0) {
      collective = true;
      i = i + 1;
    } else if (strcmp(argv[i], "--niter") == 0) {
      niter = int(atof(argv[i + 1]));
      i = i + 2;
    }
  }
  hsize_t dims_g[2] = {dims[0], dims[1] * nproc};
  /*
   * Create the data space with unlimited dimensions.
   */
  maxdims[1] = dims_g[1];
  dataspace = H5Screate_simple(RANK, dims_g, maxdims); // dims = {3, 3}
  hid_t memspace = H5Screate_simple(RANK, dims, NULL);

  /*
   * Create a new file. If file exists its contents will be overwritten.
   */

  hid_t fapl_id = H5Pcreate(H5P_FILE_ACCESS);

  H5Pset_fapl_mpio(fapl_id, comm, info);

  file = H5Fcreate(H5FILE_NAME, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id);
  /*
   * Modify dataset creation properties, i.e. enable chunking.
   */
  cparms = H5Pcreate(H5P_DATASET_CREATE);
  hid_t dxpl_id = H5Pcreate(H5P_DATASET_XFER);
  if (collective)
    H5Pset_dxpl_mpio(dxpl_id, H5FD_MPIO_COLLECTIVE);
  status = H5Pset_chunk(cparms, RANK, chunk_dims);
  status = H5Pset_fill_value(cparms, H5T_NATIVE_INT, &fillvalue);

  /*
   * Create a new dataset within the file using cparms
   * creation properties.
   */
  dataset = H5Dcreate2(file, DATASETNAME, H5T_NATIVE_INT, dataspace,
                       H5P_DEFAULT, cparms, H5P_DEFAULT);

  /*
   * Extend the dataset. This call assures that dataset is at least 3 x 3.
   */
  offset[0] = 0;
  offset[1] = rank * dims[1];
  for (int i = 0; i < niter; i++) {
    int data[3][3] = {{i + rank * 1000, i, i}, /* data to write */
                      {i, i, i},
                      {i, i, i}};

    /*
     * Select a hyperslab.
     */
    status = H5Dset_extent(dataset, dims_g);
    if (rank == 0)
      printf("%d: (%d, %d)\n", i, dims_g[0], dims_g[1]);
    dataspace = H5Dget_space(dataset);
    hsize_t count[2] = {1, 1};
    status = H5Sselect_hyperslab(dataspace, H5S_SELECT_SET, offset, NULL, dims,
                                 count);
    status =
        H5Dwrite(dataset, H5T_NATIVE_INT, memspace, dataspace, dxpl_id, data);
    offset[0] = offset[0] + dims[0];
    dims_g[0] = dims_g[0] + dims[0];
  }
  H5Dclose(dataset);
  H5Sclose(memspace);
  H5Sclose(dataspace);
  H5Pclose(cparms);

  hid_t dset2 = H5Dcreate2(file, "dset2", H5T_NATIVE_INT, dataspace,
                           H5P_DEFAULT, cparms, H5P_DEFAULT);

  /*
   * Extend the dataset. This call assures that dataset is at least 3 x 3.
   */
  offset[0] = 0;
  offset[1] = rank * dims[1];
  for (int i = 0; i < niter; i++) {
    int data[3][3] = {{i + rank * 1000, i, i}, /* data to write */
                      {i, i, i},
                      {i, i, i}};

    /*
     * Select a hyperslab.
     */
    status = H5Dset_extent(dataset, dims_g);

    if (rank == 0)
      printf("%d: (%d, %d)\n", i, dims_g[0], dims_g[1]);
    dataspace = H5Dget_space(dataset);

    hsize_t count[2] = {1, 1};
    status = H5Sselect_hyperslab(dataspace, H5S_SELECT_SET, offset, NULL, dims,
                                 count);
    H5Fcache_async_op_pause(file);
    status =
        H5Dwrite(dataset, H5T_NATIVE_INT, memspace, dataspace, dxpl_id, data);
    status = H5Dset_extent(dset2, dims_g);
    hid_t dataspace2 = H5Dget_space(dset2);
    status = H5Sselect_hyperslab(dataspace2, H5S_SELECT_SET, offset, NULL, dims,
                                 count);
    status =
        H5Dwrite(dset2, H5T_NATIVE_INT, memspace, dataspace2, dxpl_id, data);
    offset[0] = offset[0] + dims[0];
    dims_g[0] = dims_g[0] + dims[0];
    H5Fcache_async_op_start(file);
    H5Sclose(dataspace2);
  }
  H5Dclose(dataset);
  H5Dclose(dset2);
  H5Sclose(memspace);
  H5Sclose(dataspace);

  H5Pclose(cparms);

  H5Fclose(file);
  H5Pclose(dxpl_id);
  H5Pclose(fapl_id);
  MPI_Finalize();
  return 0;
}
