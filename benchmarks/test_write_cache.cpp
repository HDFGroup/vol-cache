#include "hdf5.h"
#include "mpi.h"
#include "stdlib.h"
#include "stdio.h"
#include <sys/time.h>
#include <string.h>
#include "timing.h"
#include <stdlib.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include "cache_utils.h"
#include "mpi.h"
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/statvfs.h>
#include <stdlib.h>
#include "stat.h"
#include "debug.h"
#include <unistd.h>
#include "H5LS.h"
#include "H5VLcache_ext.h"
#include "h5_async_lib.h"
void int2char(int a, char str[255]) {
  sprintf(str, "%d", a);
}
hsize_t get_buf_size(hid_t mspace, hid_t tid) {
  hsize_t nelement = H5Sget_select_npoints(mspace);
  hsize_t s = H5Tget_size(tid);
  return s*nelement;
}
void mkdirRecursive(const char *path, mode_t mode) {
    char opath[PATH_MAX];
    char *p;
    size_t len;

    strncpy(opath, path, sizeof(opath));
    opath[sizeof(opath) - 1] = '\0';
    len = strlen(opath);
    if (len == 0)
        return;
    else if (opath[len - 1] == '/')
        opath[len - 1] = '\0';
    for(p = opath; *p; p++)
        if (*p == '/') {
            *p = '\0';
            if (access(opath, F_OK))
                mkdir(opath, mode);
            *p = '/';
        }
    if (access(opath, F_OK))         /* if path is not terminated with / */
        mkdir(opath, mode);
}


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
  char ssd_cache [255] = "no";
  if (getenv("HDF5_CACHE_WR")) {
    strcpy(ssd_cache, getenv("HDF5_CACHE_WR"));
  }
  bool cache = false; 
  if (strcmp(ssd_cache, "yes")==0) {
    cache=true;
  }

  // Assuming that the dataset is a two dimensional array of 8x5 dimension;
  size_t d1 = 2048; 
  size_t d2 = 2048;
  int nvars = 8;
  int niter = 4; 
  char scratch[255] = "./";
  double sleep=0.0;
  int nw = 1;
  bool collective=false;
  for(int i=1; i<argc; i++) {
    if (strcmp(argv[i], "--dim")==0) {
      d1 = int(atoi(argv[i+1])); 
      d2 = int(atoi(argv[i+2])); 
      i+=2; 
    } else if (strcmp(argv[i], "--niter")==0) {
      niter = int(atoi(argv[i+1])); 
      i+=1; 
    } else if (strcmp(argv[i], "--scratch")==0) {
      strcpy(scratch, argv[i+1]);
      i+=1;
    } else if (strcmp(argv[i],"--sleep")==0) {
      sleep = atof(argv[i+1]); 
      i+=1;
    } else if (strcmp(argv[i],"--nvars")==0) {
      nvars = int(atof(argv[i+1])); 
      i+=1;
    } else if (strcmp(argv[i],"--nw")==0) {
      nw = int(atof(argv[i+1])); 
      i+=1; 
    } else if (strcmp(argv[i], "--collective")==0) {
      collective = true;
    }
  }
  hsize_t ldims[2] = {d1, d2};
  hsize_t oned = d1*d2;
  MPI_Comm comm = MPI_COMM_WORLD;
  MPI_Info info = MPI_INFO_NULL;
  int rank, nproc, provided; 
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  MPI_Comm_size(comm, &nproc);
  MPI_Comm_rank(comm, &rank);
  if (rank==0) cout << "MPI_Init_thread provided: " << provided << endl;
  Timing tt(rank==io_node());

  //printf("     MPI: I am rank %d of %d \n", rank, nproc);
  // find local array dimension and offset; 
  hsize_t gdims[2] = {d1*nproc, d2};
  if (rank==0) {
    printf("=============================================\n");
    printf(" Buf dim: %llu x %llu\n",  ldims[0], ldims[1]);
    printf("Buf size: %f MB\n", float(d1*d2)/1024/1024*sizeof(int));
    printf(" Scratch: %s\n", scratch); 
    printf("   nproc: %d\n", nproc);
    printf("=============================================\n");
    if (cache) printf("** using SSD as a cache **\n"); 
  }
  hsize_t offset[2] = {0, 0};
  // setup file access property list for mpio
  hid_t plist_id = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_fapl_mpio(plist_id, comm, info);
  bool p = true; 
  //H5Pset_fapl_cache(plist_id, "HDF5_CACHE_WR", &p);
  
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
  int* data = new int[ldims[0]*ldims[1]];
  // set up dataset access property list 
  hid_t dxf_id = H5Pcreate(H5P_DATASET_XFER);
  if (collective)
    H5Pset_dxpl_mpio(dxf_id, H5FD_MPIO_COLLECTIVE);
  else
    H5Pset_dxpl_mpio(dxf_id, H5FD_MPIO_INDEPENDENT);
  
  hid_t filespace = H5Screate_simple(2, gdims, NULL);
  hid_t dt = H5Tcopy(H5T_NATIVE_INT);
  hsize_t size = get_buf_size(memspace, dt);
  if (rank==0) {
    printf(" Total mspace size: %5.5f MB \n", float(size)/1024/1024);
    printf("    Number of dset: %d \n", nvars);
  }
  vector<double> t;
  t.resize(niter);
  hsize_t count[2] = {1, 1};
  tt.start_clock("total");
  tt.start_clock("H5Fcreate");   
  hid_t file_id = H5Fcreate(f, H5F_ACC_TRUNC, H5P_DEFAULT, plist_id);
  tt.stop_clock("H5Fcreate");

  for (int it = 0; it < niter; it++) {
    if (rank==0) printf("\nIter [%d]\n=============\n", it);
    hid_t *dset_id = new hid_t[nvars];
    hid_t *filespace = new hid_t[nvars];
    char str[255];
    int2char(it, str);
    hid_t grp_id = H5Gcreate(file_id, str, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    for (int i=0; i<nvars; i++) {
      filespace[i] = H5Screate_simple(2, gdims, NULL);
      char dsetn[255] = "dset-";
      char str[255];
      int2char(i, str);
      strcat(dsetn, str);
      tt.start_clock("H5Dcreate");
      dset_id[i] = H5Dcreate(grp_id, dsetn, dt, filespace[i], H5P_DEFAULT,
				H5P_DEFAULT, H5P_DEFAULT);
      tt.stop_clock("H5Dcreate"); 
    }
    hid_t memspace = H5Screate_simple(2, ldims, NULL);
    tt.start_clock("Init_array");     
    for(int j=0; j<ldims[0]*ldims[1]; j++)
      data[j] = j;
    tt.stop_clock("Init_array");
    if (rank==0) printf("pause async jobs execution\n");
    H5Fasync_op_pause(file_id);
    //H5Pset_dxpl_pause(dxf_id, true);
    for (int i=0; i<nvars; i++) {
      // select hyperslab
      // hyperslab selection
      tt.start_clock("Select");
      offset[0]= rank*ldims[0];
      H5Sselect_hyperslab(filespace[i], H5S_SELECT_SET, offset, NULL, ldims, count);
      tt.stop_clock("Select");
      // dataset write
      for (int w=0; w<nw; w++) {
	if (debug_level()>1 && rank==0)
	  printf("start dwrite timing\n");
	tt.start_clock("H5Dwrite");
	hid_t status = H5Dwrite(dset_id[i], H5T_NATIVE_INT, memspace, filespace[i], dxf_id, data); // write memory to file
	tt.stop_clock("H5Dwrite");
	if (debug_level()>1 && rank==0)
	  printf("end dwrite timing\n");
	if (rank==0) 
	  printf("  * Var(%d) -   write rate: %f MiB/s\n", i, nw*size*nproc/tt["H5Dwrite"].t_iter[it*nvars+i]/1024/1024);
      }
    }
    if (rank==0) printf("start async jobs execution\n");
    //H5Pset_dxpl_pause(dxf_id, false);
    H5Fasync_op_start(file_id, dxf_id);
    tt.start_clock("compute");
    if (debug_level()>1 && rank==0)
      printf("SLEEP START\n"); 
    msleep(int(sleep*1000));
    if (debug_level()>1 && rank==0)
      printf("SLEEP END\n"); 
    tt.stop_clock("compute");
    tt.start_clock("close"); 
    for(int i=0; i<nvars; i++) {
      tt.start_clock("H5Dclose");
      H5Sclose(filespace[i]);
      H5Dclose(dset_id[i]);
      tt.stop_clock("H5Dclose");
    }
    H5Sclose(memspace);
    tt.stop_clock("close"); 
    delete filespace; 
    delete dset_id; 
    Timer T = tt["H5Dwrite"];
    double avg = 0.0; 
    double std = 0.0; 
    stat(&T.t_iter[it*nvars], nvars, avg, std, 'n');
    t[it] = avg*nvars;  
    if (rank==0) printf("Iter [%d] write rate: %f MB/s (%f sec)\n", it, size*nproc/avg/1024/1024, t[it]);
    tt.start_clock("H5Fdelete");
    //if (rank==0) system("rm -r parallel_file.h5");
    tt.stop_clock("H5Fdelete");
    H5Gclose(grp_id);
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
  bool master = (rank==0); 
  delete [] data;
  Timer T = tt["H5Dwrite"]; 
  double avg = 0.0; 
  double std = 0.0; 
  stat(&t[0], niter, avg, std, 'i');
  if (rank==0) printf("Overall write rate: %f +/- %f MB/s\n", size*avg*nproc*nvars/1024/1024, size*nproc*std*nvars/1024/1024);
  MPI_Finalize();
  return 0;
}
