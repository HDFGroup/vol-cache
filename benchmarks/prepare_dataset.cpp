/*
  This file is for testing reading the data set in parallel in data paralle training. 
  We assume that the dataset is in a single HDF5 file. Each dataset is stored in the 
  following way: 

  (nsample, d1, d2 ..., dn)

  Each sample are an n-dimensional array 
  
  When we read the data, each rank will read a batch of sample randomly or contiguously
  from the HDF5 file. Each sample has a unique id associate with it. At the begining of 
  epoch, we mannually partition the entire dataset with nproc pieces - where nproc is 
  the number of workers. 

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
using namespace std;
#define MAXDIM 1024

void dim_dist(hsize_t gdim, int nproc, int rank, hsize_t *ldim, hsize_t *start) {
  *ldim = gdim/nproc;
  *start = *ldim*rank; 
  if (rank < gdim%nproc) {
    *ldim += 1;
    *start += rank;
  } else {
    *start += gdim%nproc;
  }
}


int main(int argc, char **argv) {
  int rank, nproc; 
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nproc);
  char fname[255] = "images.h5";
  char dataset[255] = "dataset";
  size_t num_images = 1024;
  size_t sz = 224; 
  int i=0;
  //  Timing tt(rank==0); 
  while (i<argc) {
    if (strcmp(argv[i], "--output")==0) {
      strcpy(fname, argv[i+1]); i+=2; 
    } else if (strcmp(argv[i], "--dataset")==0) {
      strcpy(dataset, argv[i+1]); i+=2; 
    } else if (strcmp(argv[i], "--num_images")==0) {
      num_images =size_t(atof(argv[i+1])); i+=2;
    } else if (strcmp(argv[i], "--sz")==0) {
      sz = size_t(atof(argv[i+1])); i+=2; 
    } else {
      i=i+1; 
    }
  }
  hid_t plist_id = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_fapl_mpio(plist_id, MPI_COMM_WORLD, MPI_INFO_NULL);
  hid_t fd = H5Fcreate(fname, H5F_ACC_TRUNC, H5P_DEFAULT, plist_id);
  hid_t dxf_id = H5Pcreate(H5P_DATASET_XFER);
  H5Pset_dxpl_mpio(dxf_id, H5FD_MPIO_COLLECTIVE);
  
  hsize_t gdims[4] = {hsize_t(num_images), sz, sz, 3}; 
  hid_t fspace = H5Screate_simple(4, gdims, NULL);  
  hsize_t ns_loc, fs_loc; 
  dim_dist(gdims[0], nproc, rank, &ns_loc, &fs_loc);
  hsize_t ldims[4] = {ns_loc, sz, sz, 3}; 
  hid_t mspace = H5Screate_simple(4, ldims, NULL);

  hsize_t offset[4] = {fs_loc, 0, 0, 0}; 
  hsize_t count[4] = {1, 1, 1, 1}; 
  if (rank==0) {
    cout << "\n====== dataset info ======" << endl; 
    cout << "Dataset file: " << fname << endl;
    cout << "Dataset: " << dataset << endl; 
    cout << "Number of samples in the dataset: " << gdims[0] << endl; 
  }
  
  float *dat = new float[ns_loc*sz*sz*3];
  for(int i=0; i<ns_loc; i++) {
    for(int j=0; j<sz*sz*3; j++)
      dat[i*sz*sz*3+j] = fs_loc + i; 
  }

  H5Sselect_hyperslab(fspace, H5S_SELECT_SET, offset, NULL, ldims, count);
  hid_t dset = H5Dcreate(fd, dataset, H5T_NATIVE_FLOAT, fspace, H5P_DEFAULT,
			 H5P_DEFAULT, H5P_DEFAULT);

  H5Dwrite(dset, H5T_NATIVE_FLOAT, mspace, fspace, H5P_DEFAULT, dat);

  H5Pclose(plist_id);
  H5Sclose(mspace);
  H5Sclose(fspace);
  H5Dclose(dset);
  H5Fclose(fd);

  delete [] dat;
  MPI_Finalize();
  return 0;
}
