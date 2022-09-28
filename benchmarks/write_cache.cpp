#include "H5LS.h"
#include "cache_new_h5api.h"
#include "cache_utils.h"
#include "debug.h"
#include "hdf5.h"
#include "mpi.h"
#include "stat.h"
#include "stdio.h"
#include "stdlib.h"
#include "timing.h"
#include <assert.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/time.h>
#include <unistd.h>
//#include "h5_async_lib.h"
int msleep(long miliseconds) {
  struct timespec req, rem;

  if (miliseconds > 999) {
    req.tv_sec = (int)(miliseconds / 1000); /* Must be Non-Negative */
    req.tv_nsec = (miliseconds - ((long)req.tv_sec * 1000)) *
                  1000000; /* Must be in range of 0 to 999999999 */
  } else {
    req.tv_sec = 0; /* Must be Non-Negative */
    req.tv_nsec =
        miliseconds * 1000000; /* Must be in range of 0 to 999999999 */
  }
  return nanosleep(&req, &rem);
}

int main(int argc, char **argv) {
  char ssd_cache[255] = "no";
  if (getenv("HDF5_CACHE_WR")) {
    strcpy(ssd_cache, getenv("HDF5_CACHE_WR"));
  }
  bool barrier = false;
  bool cache = false;
  if (strcmp(ssd_cache, "yes") == 0) {
    cache = true;
  }

  // Assuming that the dataset is a two dimensional array of 8x5 dimension;
  size_t d1 = 2048;
  size_t d2 = 2048;
  int nvars = 8;
  int niter = 4;
  char scratch[255] = "./";
  double sleep = 0.0;
  int nw = 1;
  bool collective = false;
  bool fdelete = false;
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--dim") == 0) {
      d1 = int(atoi(argv[i + 1]));
      d2 = int(atoi(argv[i + 2]));
      i += 2;
    } else if (strcmp(argv[i], "--niter") == 0) {
      niter = int(atoi(argv[i + 1]));
      i += 1;
    } else if (strcmp(argv[i], "--scratch") == 0) {
      strcpy(scratch, argv[i + 1]);
      i += 1;
    } else if (strcmp(argv[i], "--sleep") == 0) {
      sleep = atof(argv[i + 1]);
      i += 1;
    } else if (strcmp(argv[i], "--nvars") == 0) {
      nvars = int(atof(argv[i + 1]));
      i += 1;
    } else if (strcmp(argv[i], "--nw") == 0) {
      nw = int(atof(argv[i + 1]));
      i += 1;
    } else if (strcmp(argv[i], "--collective") == 0) {
      collective = true;
    } else if (strcmp(argv[i], "--barrier") == 0) {
      barrier = true;
    } else if (strcmp(argv[i], "--fdelete") == 0) {
      fdelete = true;
    }
  }
  hsize_t ldims[2] = {d1, d2};
  hsize_t oned = d1 * d2;
  MPI_Comm comm = MPI_COMM_WORLD;
  MPI_Info info = MPI_INFO_NULL;
  int rank, nproc, provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  MPI_Comm_size(comm, &nproc);
  MPI_Comm_rank(comm, &rank);
  assert(provided == 3);
  Timing tt(rank == io_node());

  // printf("     MPI: I am rank %d of %d \n", rank, nproc);
  // find local array dimension and offset;
  hsize_t gdims[2] = {d1 * nproc, d2};
  if (rank == 0) {
    printf("=============================================\n");
    printf(" Buf dim: %llu x %llu\n", ldims[0], ldims[1]);
    printf("Buf size: %f MB\n", float(d1 * d2) / 1024 / 1024 * sizeof(int));
    printf(" Scratch: %s\n", scratch);
    printf("   nproc: %d\n", nproc);
    printf("=============================================\n");
    if (cache)
      printf("** using SSD as a cache **\n");
  }
  hsize_t offset[2] = {0, 0};
  // setup file access property list for mpio
  hid_t plist_id = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_fapl_mpio(plist_id, comm, info);
  bool p = true;
  // H5Pset_fapl_cache(plist_id, "HDF5_CACHE_WR", &p);

  if (getenv("ALIGNMENT")) {
    if (rank == 0)
      printf("Set Alignment: %s\n", getenv("ALIGNMENT"));
    H5Pset_alignment(plist_id, 0, int(atof(getenv("ALIGNMENT"))));
  }
  char f[255];
  strcpy(f, scratch);
  mkdirRecursive(scratch, 755);
  strcat(f, "/parallel_file.h5");
  // create memory space
  hid_t memspace = H5Screate_simple(2, ldims, NULL);
  // define local data
  int *data = new int[ldims[0] * ldims[1]];
  // set up dataset access property list
  hid_t dxf_id = H5Pcreate(H5P_DATASET_XFER);
  if (collective)
    H5Pset_dxpl_mpio(dxf_id, H5FD_MPIO_COLLECTIVE);
  else
    H5Pset_dxpl_mpio(dxf_id, H5FD_MPIO_INDEPENDENT);

  hid_t filespace = H5Screate_simple(2, gdims, NULL);
  hid_t dt = H5Tcopy(H5T_NATIVE_INT);
  hsize_t size = get_buf_size(memspace, dt);
  if (rank == 0) {
    printf(" Total mspace size: %5.5f MB \n", float(size) / 1024 / 1024);
    printf("    Number of dset: %d \n", nvars);
  }
  vector<double> t;
  t.resize(niter);
  hsize_t count[2] = {1, 1};
  tt.start_clock("total");
  tt.start_clock("H5Fcreate");
  hid_t file_id = H5Fcreate(f, H5F_ACC_TRUNC, H5P_DEFAULT, plist_id);
  tt.stop_clock("H5Fcreate");
  H5Fcache_async_close_set(file_id);
  for (int it = 0; it < niter; it++) {
    tt.start_clock("H5Fcache_wait");
    H5Fcache_async_close_wait(file_id);
    tt.stop_clock("H5Fcache_wait");
    if (rank == 0)
      printf("\nIter [%d]\n=============\n", it);
    hid_t *dset_id = new hid_t[nvars];
    hid_t *filespace = new hid_t[nvars];
    char str[255];
    int2char(it, str);
    hid_t grp_id =
        H5Gcreate(file_id, str, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    for (int i = 0; i < nvars; i++) {
      filespace[i] = H5Screate_simple(2, gdims, NULL);
      char dsetn[255] = "dset-";
      char str[255];
      int2char(i, str);
      strcat(dsetn, str);
      tt.start_clock("H5Dcreate");
      dset_id[i] = H5Dcreate(grp_id, dsetn, dt, filespace[i], H5P_DEFAULT,
                             H5P_DEFAULT, H5P_DEFAULT);
      tt.stop_clock("H5Dcreate");
      tt.start_clock("Select");
      offset[0] = rank * ldims[0];
      H5Sselect_hyperslab(filespace[i], H5S_SELECT_SET, offset, NULL, ldims,
                          count);
      tt.stop_clock("Select");
    }
    hid_t memspace = H5Screate_simple(2, ldims, NULL);
    tt.start_clock("Init_array");
    for (int j = 0; j < ldims[0] * ldims[1]; j++)
      data[j] = j;
    tt.stop_clock("Init_array");
#ifndef NDEBUG
    if (rank == 0 and debug_level() > 1)
      printf("pause async jobs execution\n");
#endif
    H5Fcache_async_op_pause(file_id);
    for (int i = 0; i < nvars; i++) {
      // dataset write
      for (int w = 0; w < nw; w++) {
#ifndef NDEBUG
        if (debug_level() > 1 && rank == 0)
          printf("start dwrite timing\n");
#endif
        tt.start_clock("H5Dwrite");
        hid_t status =
            H5Dwrite(dset_id[i], H5T_NATIVE_INT, memspace, filespace[i], dxf_id,
                     data); // write memory to file
        tt.stop_clock("H5Dwrite");
#ifndef NDEBUG
        if (debug_level() > 1 && rank == 0)
          printf("end dwrite timing\n");
#endif
        if (rank == 0)
          printf("  * Var(%d) -   raw write rate: %f MiB/s\n", i,
                 nw * size * nproc / tt["H5Dwrite"].t_iter[it * nvars + i] /
                     1024 / 1024);
      }
    }
#ifndef NDEBUG
    if (rank == 0 and debug_level() > 1)
      printf("start async jobs execution\n");
#endif
    H5Fcache_async_op_start(file_id);
    tt.start_clock("compute");
#ifndef NDEBUG
    if (debug_level() > 1 && rank == 0)
      printf("SLEEP START\n");
#endif
    msleep(int(sleep * 1000));
#ifndef NDEBUG
    if (debug_level() > 1 && rank == 0)
      printf("SLEEP END\n");
#endif
    tt.stop_clock("compute");
    tt.start_clock("barrier");
    if (barrier)
      MPI_Barrier(MPI_COMM_WORLD);
    tt.stop_clock("barrier");
    tt.start_clock("close");
    for (int i = 0; i < nvars; i++) {
      tt.start_clock("H5Dclose");
      H5Dclose(dset_id[i]);
      tt.stop_clock("H5Dclose");
      tt.start_clock("H5Sclose");
      H5Sclose(filespace[i]);
      tt.stop_clock("H5Sclose");
    }
    tt.start_clock("H5Sclose");
    H5Sclose(memspace);
    tt.stop_clock("H5Sclose");
    tt.start_clock("H5Gclose");
    H5Gclose(grp_id);
    tt.stop_clock("H5Gclose");
    tt.stop_clock("close");
    delete[] filespace;
    delete[] dset_id;
    Timer T = tt["H5Dwrite"];
    double avg = 0.0;
    double std = 0.0;
    stat(&T.t_iter[it * nvars], nvars, avg, std, 'n');
    t[it] = avg * nvars;
    if (rank == 0)
      printf("Iter [%d] raw write rate: %f MB/s (%f sec)\n", it,
             size * nproc / avg / 1024 / 1024, t[it]);
  }
  tt.start_clock("H5Fflush");
  H5Fflush(file_id, H5F_SCOPE_LOCAL);
  tt.stop_clock("H5Fflush");
  tt.start_clock("H5Fclose");
  H5Fclose(file_id);
  tt.stop_clock("H5Fclose");
  H5Pclose(dxf_id);
  H5Pclose(plist_id);
  tt.stop_clock("total");
  bool master = (rank == 0);
  delete[] data;
  Timer T = tt["H5Dwrite"];
  double avg = 0.0;
  double std = 0.0;
  stat(&t[0], niter, avg, std, 'i');
  double raw_time = tt["H5Dwrite"].t;

  if (rank == 0)
    printf("Overall raw write rate: %f MB/s\n",
           size / raw_time * nproc * nvars / 1024 / 1024 * niter);

  double total_time = tt["H5Dwrite"].t + tt["H5Fcreate"].t + tt["H5Gcreate"].t +
                      tt["H5Gclose"].t + tt["H5Dclose"].t + tt["H5Fclose"].t +
                      tt["H5Fflush"].t + tt["H5Gclose"].t;
  if (rank == 0) {
    printf("Overall observed write rate: %f MB/s\n",
           size / total_time * nproc * nvars / 1024 / 1024 * niter);
    printf("Overall observed write rate (sync): %f MB/s\n",
           size / (tt["total"].t - tt["compute"].t) * nproc * nvars / 1024 /
               1024 * niter);
  }
  if (fdelete) {
    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0)
      system("rm -r parallel_file.h5");
  }
  MPI_Finalize();
  return 0;
}
