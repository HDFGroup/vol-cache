/*
  This code is to prototying the idea of incorparating node-local storage 
  into repeatedly read workflow. We assume that the application is reading
  the same dataset periodically from the file system. Out idea is to bring 
  the data to the node-local storage in the first iteration, and read from 
  the node-local storage directly and subsequent iterations. 

  To start with, we assume that the entire dataset fit into the node-local
  storage. We also assume that the dataset is stored in the following format: 

  (nsample, d1, d2 ..., dn), where each sample is an n-dimensional array. 

  When reading the data, each rank gets a batch of sample randomly or contiguously
  from the HDF5 file through hyperslab selection. 

  Huihuo Zheng @ ALCF
  Revision history: 
  Mar 8, 2020, added MPI_Put and MPI_Get
  Mar 1, 2020: added MPIIO support
  Feb 29, 2020: Added debug info support.
  Feb 28, 2020: Created with simple information. 
 */
#include <iostream>
#include "hdf5.h"
#include "mpi.h"
#include "stdlib.h"
#include "string.h"
#include "stdio.h"
#include <random>
#include <algorithm>
#include <vector>
#include "timing.h"
#include <assert.h>
#include "debug.h"
#include <unistd.h>
// POSIX I/O
#include <sys/stat.h> 
#include <fcntl.h>
// Memory map
#include <sys/mman.h>
#include "H5Dio_cache.h"
#include "H5VLpassthru_ext.h"
extern H5Dread_cache_metadata H5DRMM; 
using namespace std;

int msleep(long miliseconds)
{
  struct timespec req, rem;

  if(miliseconds > 999)
    {   
      req.tv_sec = (int)(miliseconds / 1000);                            /* Must be Non-Negative */
      req.tv_nsec = (miliseconds - ((long)req.tv_sec * 1000)) * 1000000; /* Must be in range of 0 to 999999999 */
    }   
  else
    {   
      req.tv_sec = 0;                         /* Must be Non-Negative */
      req.tv_nsec = miliseconds * 1000000;    /* Must be in range of 0 to 999999999 */
    }   
  return nanosleep(&req , &rem);
}

int main(int argc, char **argv) {
  int rank, nproc;
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nproc);
  double compute = 0.0; 
  char fname[255] = "./images.h5";
  char dataset[255] = "dataset";
  char local_storage[255] = "./";
  bool shuffle = false;
  bool mpio_collective = false; 
  bool mpio_independent = false;
  bool cache = false; 
  int epochs = 4;
  int num_batches = 16;
  int batch_size = 32; 
  int rank_shift = 0;
  int num_images = 1;
  bool barrier = false; // set this always to be false. this is just for debug purpose
  bool remap = false; 
  int i=0;
  Timing tt(io_node()==rank);
  // Input 
  while (i<argc) {
    if (strcmp(argv[i], "--input")==0) {
      strcpy(fname, argv[i+1]); i+=2; 
    } else if (strcmp(argv[i], "--dataset")==0) {
      strcpy(dataset, argv[i+1]); i+=2; 
    } else if (strcmp(argv[i], "--num_batches")==0) {
      num_batches = int(atof(argv[i+1])); i+=2;
    } else if (strcmp(argv[i], "--batch_size")==0) {
      batch_size = int(atof(argv[i+1])); i+=2; 
    } else if (strcmp(argv[i], "--shuffle")==0) {
      shuffle = true; i=i+1; 
    } else if (strcmp(argv[i], "--mpio_independent")==0) {
      mpio_independent = true; i=i+1; 
    } else if (strcmp(argv[i], "--mpio_collective")==0) {
      mpio_collective = true; i=i+1; 
    } else if (strcmp(argv[i], "--epochs") == 0) {
      epochs = int(atof(argv[i+1])); i+=2;
    } else if (strcmp(argv[i], "--rank_shift")==0) {
      rank_shift = int(atof(argv[i+1])); i+=2;
    } else if (strcmp(argv[i], "--cache") ==0){
      cache = true; i=i+1; 
    } else if (strcmp(argv[i], "--remap") == 0) {
      remap = true; i=i+1; 
    } else if (strcmp(argv[i], "--local_storage")==0) {
      strcpy(local_storage, argv[i+1]); i+=2;
    } else if (strcmp(argv[i], "--compute")==0) {
      compute = atof(argv[i+1]); i+=2;
    } else if (strcmp(argv[i], "--barrier")==0) {
      barrier = true; i = i+1;
    } else {
      i=i+1; 
    }
  }
  hid_t plist_id = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_fapl_mpio(plist_id, MPI_COMM_WORLD, MPI_INFO_NULL);
  hid_t fd;
  fd = H5Fopen(fname, H5F_ACC_RDONLY, plist_id);
  hsize_t s;
  //  H5Freserve_cache(fd, H5P_DEFAULT, NULL, 1048576);
  //H5Fquery_cache(fd, H5P_DEFAULT, NULL, &s);
  //  printf("size: %lld\n", s);
  hid_t dset;
  tt.start_clock("H5Dopen"); 
  dset = H5Dopen(fd, dataset, H5P_DEFAULT);
  tt.stop_clock("H5Dopen");
  hid_t fspace = H5Dget_space(dset);

  int ndims = H5Sget_simple_extent_ndims(fspace);
  hsize_t *gdims = new hsize_t [ndims];
  H5Sget_simple_extent_dims(fspace, gdims, NULL);

  hsize_t dim = 1; // compute the size of a single sample
  hsize_t *ldims = new hsize_t [ndims]; // for one batch of data
  for(int i=0; i<ndims; i++) {
    dim = dim*gdims[i];
    ldims[i] = gdims[i];
  }
  dim = dim/gdims[0];
  num_images = batch_size * num_batches * nproc;
  if (num_images > gdims[0]) num_batches = gdims[0]/batch_size/nproc; 
  if (io_node()==rank) {
    cout << "\n====== dataset info ======" << endl; 
    cout << "Dataset file: " << fname << endl;
    cout << "Dataset name: " << dataset << endl; 
    cout << "Number of samples in the dataset: " << gdims[0] << endl;
    cout << "Number of images selected: " << num_images << endl; 
    cout << "Dimension of the sample: " << ndims - 1 << endl;
    cout << "Size in each dimension: "; 
    for (int i=1; i<ndims; i++) {
      cout << " " << gdims[i];
    }
    cout << endl;
    cout << "\n====== I/O & MPI info ======" << endl; 
    cout << "MPIO_COLLECTIVE: " << mpio_collective << endl; 
    cout << "MPIO_INDEPENDENT: " << mpio_independent << endl; 
    cout << "\n====== training info ======" << endl; 
    cout << "Batch size: " << batch_size << endl; 
    cout << "Number of batches per epoch: " << num_batches << endl;
    cout << "Number of epochs: " << epochs << endl;
    cout << "Shuffling the samples: " << shuffle << endl;
    cout << "Number of workers: " << nproc << endl;
    cout << "Training time per batch: " << compute << endl; 
    cout << "\n======= Local storage path =====" << endl; 
    cout << "Path (MEMORY means reading everything to memory directly): " << local_storage << endl;
    cout << endl;
  }

  // sample indices

  vector<int> id;
  id.resize(num_images);
  for(int i=0; i<num_images; i++) id[i] = i;
  mt19937 g(100);

  size_t ns_loc, fs_loc; // number of sample per worker, first sample
  parallel_dist(num_images, nproc, rank, &ns_loc, &fs_loc);

  // buffer for loading one batch of data
  float *dat = new float[dim*batch_size];// buffer to store one batch of data
  ldims[0] = batch_size;

  hid_t mspace = H5Screate_simple(ndims, ldims, NULL); // memory space for one bach of data
  hid_t dxf_id = H5Pcreate(H5P_DATASET_XFER);
  if (mpio_collective) {
    H5Pset_dxpl_mpio(dxf_id, H5FD_MPIO_COLLECTIVE);
  } else if (mpio_independent) {
    H5Pset_dxpl_mpio(dxf_id, H5FD_MPIO_INDEPENDENT); 
  } 

  // First epoch -- reading the data from the file system and cache it to local storage
  if (shuffle) ::shuffle(id.begin(), id.end(), g);
  for(int e =0; e < epochs; e++) {
    double vm, rss; 

    if (shuffle) ::shuffle(id.begin(), id.end(), g);
    parallel_dist(num_images, nproc, (rank+e*rank_shift)%nproc, &ns_loc, &fs_loc);
    double t1 = 0.0;
    for (int nb = 0; nb < num_batches; nb++) {
      vector<int> b = vector<int> (id.begin() + fs_loc+nb*batch_size, id.begin() + fs_loc+(nb+1)*batch_size);
      sort(b.begin(), b.end());
      //      if (io_node()==rank and debug_level() > 1) cout << "Batch: " << nb << endl;
      double t0 = MPI_Wtime();
      tt.start_clock("Select");
      set_hyperslab_from_samples(&b[0], batch_size, &fspace); 
      tt.stop_clock("Select");
      tt.start_clock("H5Dread");
      H5Dread(dset, H5T_NATIVE_FLOAT, mspace, fspace, dxf_id, dat);
      tt.stop_clock("H5Dread");
      t1 += MPI_Wtime() - t0;
      msleep(int(compute*1000)); 
      if (io_node()==rank and debug_level()>1) {
	for(int i=0; i<batch_size; i++) {
	  cout << "  " << dat[i*dim] << "(" << b[i] << ")  ";
	  if (i%5==4) cout << endl; 
	}
	cout << endl;
      }
    }
    if (io_node()==rank) 
      printf("Epoch: %d  ---  time: %6.2f (sec) --- throughput: %6.2f (imgs/sec) --- rate: %6.2f (MB/sec)\n",
	     e, t1, nproc*num_batches*batch_size/t1,
	     num_batches*batch_size*dim*sizeof(float)/t1/1024/1024*nproc);
  }
  H5Dclose(dset);
  H5Pclose(plist_id);
  H5Sclose(mspace);
  H5Sclose(fspace);
  H5Fclose(fd);
  delete [] dat;
  delete [] ldims;
  MPI_Finalize();
  return 0;
}
