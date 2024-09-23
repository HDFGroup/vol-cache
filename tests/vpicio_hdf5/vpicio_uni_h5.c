/****** Copyright Notice ***
 *
 * PIOK - Parallel I/O Kernels - VPIC-IO, VORPAL-IO, and GCRM-IO, Copyright
 * (c) 2015, The Regents of the University of California, through Lawrence
 * Berkeley National Laboratory (subject to receipt of any required
 * approvals from the U.S. Dept. of Energy).  All rights reserved.
 *
 * If you have questions about your rights to use or distribute this
 * software, please contact Berkeley Lab's Innovation & Partnerships Office
 * at  IPO@lbl.gov.
 *
 * NOTICE.  This Software was developed under funding from the U.S.
 * Department of Energy and the U.S. Government consequently retains
 * certain rights. As such, the U.S. Government has been granted for itself
 * and others acting on its behalf a paid-up, nonexclusive, irrevocable,
 * worldwide license in the Software to reproduce, distribute copies to the
 * public, prepare derivative works, and perform publicly and display
 * publicly, and to permit other to do so.
 *
 ****************************/

/**
 *
 * Email questions to SByna@lbl.gov
 * Scientific Data Management Research Group
 * Lawrence Berkeley National Laboratory
 *
 */

// Description: This is a simple benchmark based on VPIC's I/O interface
//		Each process writes a specified number of particles into
//		a hdf5 output file using only HDF5 calls
// Author:	Suren Byna <SByna@lbl.gov>
//		Lawrence Berkeley National Laboratory, Berkeley, CA
// Created:	in 2011
// Modified:	01/06/2014 --> Removed all H5Part calls and using HDF5 calls
//

#include "hdf5.h"
#include <math.h>
#include <stdio.h>
#include <stdlib.h>

// A simple timer based on gettimeofday
#include "./timer.h"
struct timeval start_time[10];
float elapse[10];

//#define USE_COLLECTIVE

// HDF5 specific declarations
herr_t ierr;
hid_t file_id, dset_id;
hid_t filespace, memspace;
hid_t plist_id;

// Variables and dimensions
long numparticles = 8388608; // 8  meg particles per process
long long total_particles, offset;

float *x, *y, *z;
float *px, *py, *pz;
int *id1, *id2;
int x_dim = 64;
int y_dim = 64;
int z_dim = 64;

// Uniform random number
inline double uniform_random_number() {
  return (((double)rand()) / ((double)(RAND_MAX)));
}

// Initialize particle data
void init_particles() {
  int i;
  for (i = 0; i < numparticles; i++) {
    id1[i] = i;
    id2[i] = i * 2;
    x[i] = uniform_random_number() * x_dim;
    y[i] = uniform_random_number() * y_dim;
    z[i] = ((double)id1[i] / numparticles) * z_dim;
    px[i] = uniform_random_number() * x_dim;
    py[i] = uniform_random_number() * y_dim;
    pz[i] = ((double)id2[i] / numparticles) * z_dim;
  }
}

// Create HDF5 file and write data
void create_and_write_synthetic_h5_data(int rank) {
  // Note: printf statements are inserted basically
  // to check the progress. Other than that they can be removed
  dset_id = H5Dcreate(file_id, "x", H5T_NATIVE_FLOAT, filespace, H5P_DEFAULT,
                      H5P_DEFAULT, H5P_DEFAULT);
  timer_on(3);
  ierr = H5Dwrite(dset_id, H5T_NATIVE_FLOAT, memspace, filespace, plist_id, x);
  timer_off(3);
  timer_on(4);
  H5Dclose(dset_id);
  timer_off(4);
  if (rank == 0)
    printf("Written variable 1 \n");

  dset_id = H5Dcreate(file_id, "y", H5T_NATIVE_FLOAT, filespace, H5P_DEFAULT,
                      H5P_DEFAULT, H5P_DEFAULT);
  timer_on(3);
  ierr = H5Dwrite(dset_id, H5T_NATIVE_FLOAT, memspace, filespace, plist_id, y);
  timer_off(3);
  timer_on(4);
  H5Dclose(dset_id);
  timer_off(4);
  if (rank == 0)
    printf("Written variable 2 \n");

  dset_id = H5Dcreate(file_id, "z", H5T_NATIVE_FLOAT, filespace, H5P_DEFAULT,
                      H5P_DEFAULT, H5P_DEFAULT);
  timer_on(3);
  ierr = H5Dwrite(dset_id, H5T_NATIVE_FLOAT, memspace, filespace, plist_id, z);
  timer_off(3);
  timer_on(4);
  H5Dclose(dset_id);
  timer_off(4);
  if (rank == 0)
    printf("Written variable 3 \n");

  dset_id = H5Dcreate(file_id, "id1", H5T_NATIVE_INT, filespace, H5P_DEFAULT,
                      H5P_DEFAULT, H5P_DEFAULT);
  timer_on(3);
  ierr = H5Dwrite(dset_id, H5T_NATIVE_INT, memspace, filespace, plist_id, id1);
  timer_off(3);
  timer_on(4);
  H5Dclose(dset_id);
  timer_off(4);
  if (rank == 0)
    printf("Written variable 4 \n");
  dset_id = H5Dcreate(file_id, "id2", H5T_NATIVE_INT, filespace, H5P_DEFAULT,
                      H5P_DEFAULT, H5P_DEFAULT);
  timer_on(3);
  ierr = H5Dwrite(dset_id, H5T_NATIVE_INT, memspace, filespace, plist_id, id2);
  timer_off(3);

  timer_on(4);
  H5Dclose(dset_id);
  timer_off(4);
  if (rank == 0)
    printf("Written variable 5 \n");

  dset_id = H5Dcreate(file_id, "px", H5T_NATIVE_FLOAT, filespace, H5P_DEFAULT,
                      H5P_DEFAULT, H5P_DEFAULT);
  timer_on(3);
  ierr = H5Dwrite(dset_id, H5T_NATIVE_FLOAT, memspace, filespace, plist_id, px);
  timer_off(3);

  timer_on(4);
  H5Dclose(dset_id);
  timer_off(4);
  if (rank == 0)
    printf("Written variable 6 \n");

  dset_id = H5Dcreate(file_id, "py", H5T_NATIVE_FLOAT, filespace, H5P_DEFAULT,
                      H5P_DEFAULT, H5P_DEFAULT);
  timer_on(3);
  ierr = H5Dwrite(dset_id, H5T_NATIVE_FLOAT, memspace, filespace, plist_id, py);
  timer_off(3);

  timer_on(4);
  H5Dclose(dset_id);
  timer_off(4);
  if (rank == 0)
    printf("Written variable 7 \n");

  dset_id = H5Dcreate(file_id, "pz", H5T_NATIVE_FLOAT, filespace, H5P_DEFAULT,
                      H5P_DEFAULT, H5P_DEFAULT);

  timer_on(3);
  ierr = H5Dwrite(dset_id, H5T_NATIVE_FLOAT, memspace, filespace, plist_id, pz);
  timer_off(3);

  timer_on(4);
  H5Dclose(dset_id);
  timer_off(4);
  if (rank == 0)
    printf("Written variable 8 \n");
}

int main(int argc, char *argv[]) {
  char *file_name = argv[1];
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

  // MPI_Init(&argc,&argv);
  int my_rank, num_procs;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

  MPI_Comm comm = MPI_COMM_WORLD;
  MPI_Info info = MPI_INFO_NULL;

  if (argc == 3) {
    numparticles = (atoi(argv[2])) * 1024 * 1024;
  } else {
    numparticles = 8 * 1024 * 1024;
  }

  if (my_rank == 0) {
    printf("Number of particles: %ld \n", numparticles);
  }

  x = (float *)malloc(numparticles * sizeof(double));
  y = (float *)malloc(numparticles * sizeof(double));
  z = (float *)malloc(numparticles * sizeof(double));

  px = (float *)malloc(numparticles * sizeof(double));
  py = (float *)malloc(numparticles * sizeof(double));
  pz = (float *)malloc(numparticles * sizeof(double));

  id1 = (int *)malloc(numparticles * sizeof(int));
  id2 = (int *)malloc(numparticles * sizeof(int));

  init_particles();

  if (my_rank == 0) {
    printf("Finished initializing particles \n");
  }

  // h5part_int64_t alignf = 8*1024*1024;

  MPI_Barrier(MPI_COMM_WORLD);
  timer_on(0);

  MPI_Allreduce(&numparticles, &total_particles, 1, MPI_LONG_LONG, MPI_SUM,
                comm);
  MPI_Scan(&numparticles, &offset, 1, MPI_LONG_LONG, MPI_SUM, comm);
  offset -= numparticles;

  plist_id = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_fapl_mpio(plist_id, comm, info);

  // file = H5PartOpenFileParallel (file_name, H5PART_WRITE |
  // H5PART_VFD_MPIPOSIX | H5PART_FS_LUSTRE, MPI_COMM_WORLD);
  file_id = H5Fcreate(file_name, H5F_ACC_TRUNC, H5P_DEFAULT, plist_id);
  H5Pclose(plist_id);

  if (my_rank == 0) {
    printf("Opened HDF5 file... \n");
  }
  // Throttle and see performance
  // H5PartSetThrottle (file, 10);

  // H5PartWriteFileAttribString(file, "Origin", "Tested by Suren");

  filespace = H5Screate_simple(1, (hsize_t *)&total_particles, NULL);
  memspace = H5Screate_simple(1, (hsize_t *)&numparticles, NULL);

  plist_id = H5Pcreate(H5P_DATASET_XFER);
#ifdef USE_COLLECTIVE
  H5Pset_dxpl_mpio(plist_id, H5FD_MPIO_COLLECTIVE);
#else
  H5Pset_dxpl_mpio(plist_id, H5FD_MPIO_INDEPENDENT);
#endif
  H5Sselect_hyperslab(filespace, H5S_SELECT_SET, (hsize_t *)&offset, NULL,
                      (hsize_t *)&numparticles, NULL);

  MPI_Barrier(MPI_COMM_WORLD);
  timer_on(1);

  if (my_rank == 0)
    printf("Before writing particles \n");
  create_and_write_synthetic_h5_data(my_rank);

  MPI_Barrier(MPI_COMM_WORLD);
  timer_off(1);
  if (my_rank == 0)
    printf("After writing particles \n");

  timer_on(2);
  H5Fflush(file_id, H5F_SCOPE_LOCAL);
  timer_off(2);

  H5Sclose(memspace);
  H5Sclose(filespace);
  H5Pclose(plist_id);
  H5Fclose(file_id);
  if (my_rank == 0)
    printf("After closing HDF5 file \n");

  free(x);
  free(y);
  free(z);
  free(px);
  free(py);
  free(pz);
  free(id1);
  free(id2);

  MPI_Barrier(MPI_COMM_WORLD);

  timer_off(0);

  if (my_rank == 0) {
    printf("\nTiming results\n");
    timer_msg(1, "just writing data");
    timer_msg(3, "H5Dwrite");
    timer_msg(4, "H5Dclose");
    timer_msg(2, "flushing data");
    timer_msg(0, "opening, writing, flushing, and closing file");
    printf("\n");
  }

  MPI_Finalize();

  return 0;
}
