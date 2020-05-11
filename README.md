# Incorparating node local storage in HDF5

This folder contains the prototype of system-aware HDF5 incoroprating node-local storage. This is part of the ExaHDF5 ECP project lead by Suren Byna <sbyna@lbl.gov>. 

## Building the VOL
### HDF5 Dependency

This is the HDF5 cache VOL connector built based on external pass through VOL connector. 

This VOL connector was tested with the version of the HDF5 async branch as of May 11, 2020

**Note**: Make sure you have libhdf5 shared dynamic libraries in your hdf5/lib. For Linux, it's libhdf5.so, for OSX, it's libhdf5.dylib.

### Generate HDF5 shared library
If you don't have the shared dynamic libraries, you'll need to reinstall HDF5.
- Get the latest version of the develop branch;
- In the repo directory, run ./autogen.sh
- In your build directory, run configure and make sure you **DO NOT** have the option "--disable-shared", for example:
    >    env CC=mpicc ../hdf5_dev/configure --enable-build-mode=debug --enable-internal-debug=all --enable-parallel --enable-threadsafety
- make; make install

### Settings
Change following paths in Makefile:

- **HDF5_DIR**: path to your hdf5 install/build location, such as hdf5_build/hdf5/
- **SRC_DIR**: path to this VOL connector source code directory.

### Build the pass-through VOL library and run the demo
Type *make* in the source dir and you'll see **libh5passthrough_vol.so**, which is the pass -hrough VOL connector library.
To run the demo, set following environment variables first:
>
    export HDF5_PLUGIN_PATH=PATH_TO_YOUR_pass_through_vol
    export HDF5_VOL_CONNECTOR="pass_through_ext under_vol=0;under_info={};"
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:PATH_TO_YOUR_hdf5_build/hdf5/lib:$HDF5_PLUGIN_PATH

By default, the debugging mode is enabled to ensure the VOL connector is working. To disable it, simply remove the $(DEBUG) option from the CC line, and rerun make.

## Parallel HDF5 Write incorporating node-local storage
   test_write_cache.cpp is the benchmark code for evaluating the performance. In this testing case, each MPI rank has a local
   buffer BI to be written into a HDF5 file organized in the following way: [B0|B1|B2|B3]|[B0|B1|B2|B3]|...|[B0|B1|B2|B3]. The repeatition of [B0|B1|B2|B3] is the number of iterations
   * --dim: dimension of the 2D array [BI] // this is the local buffer size
   * --niter: number of iterations. Notice that the data is accumulately written to the file. 
   * --scratch: the location of the raw data
   * --sleep: sleep between different iterations

In this benchmark code, one can turns on the SSD cache effect by setting the environmental variable to SSD_CACH=yes.
SSD_PATH -- environmental variable setting the path of the SSD. 

