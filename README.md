# HDF5 Cache VOL: Efficient Parallel I/O through Caching Data on Fast Storage Layers

Documentation: https://vol-cache.readthedocs.io

This is the public repo for Cache VOL, a software package developed in the ```ExaIO``` Exascale Computing Project. The main objective of Cache VOL is to incorporate fast storage layers (e.g, burst buffer, node-local storage) into parallel I/O workflow for caching and staging data to improve the I/O efficiency. 

The design, implementation, and performance evaluation of Cache VOL is presented in our CCGrid'2022 paper: 
Huihuo Zheng, Venkatram Vishwanath, Quincey Koziol, Houjun Tang, John Ravi, John Mainzer, Suren Byna, "HDF5 Cache VOL: Efficient and Scalable Parallel
I/O through Caching Data on Node-local Storage," 2022 IEEE/ACM 21st International Symposium on Cluster, Cloud and Internet Computing (CCGrid), 2022, doi:10.1109/CCGrid54584.2022.00015

## Files under this folder
* ./src - Cache VOL source files
   * cache_utils.c, cache_utils.h --  utility functions
   * H5VLcache_ext.c, H5VLcache_ext.h -- cache VOL
   * H5LS.c, H5LS.h -- functions for managing cache storage
   * cache_new_h5api.h, cache_new_h5api.c -- new public API functions specific to the cache VOL. 
   
* ./benchmarks - microbenchmark codes
   * write_cache.cpp -- testing code for parallel write
   * read_cache.cpp, read_cache.py -- benchmark code for parallel read
   
* ./docs/ - Documentation
   * cache_vol.tex (OLD) -- prototype design based on explicit APIs and initial performance evaluation.
   * readthedoc -- https://vol-cache.readthedocs.io
* tests: this contains a set of tests for different functions. 

## Building the Cache VOL

We outline below some basic information about how to use Cache VOL. Please find detailed instruction on https://vol-cache.readthedocs.io. 

In order for cmake to find the dependent libraries, the user have to define the following environment variables
```bash
HDF5_DIR # prefix for install the HDF5 library
ABT_DIR # prefix for install the Argobots library
HDF5_VOL_DIR # prefix for install the VOL connectors
```

### Building HDF5 shared library
Currently, the cache VOL depends on the develop branch of HDF5, 
```bash 
git clone -b develop https://github.com/HDFGroup/hdf5.git
cd hdf5
./autogen.sh
./configure --prefix=$HDF5_DIR --enable-parallel --enable-threadsafe --enable-unsupported CC=mpicc
make all install 
```
When running configure, ake sure you **DO NOT** have the option "--disable-shared". 

### Build Argobots library 
```bash
git clone https://github.com/pmodels/argobots.git
cd argobots
./autogen
./configure --prefix=$ABT_DIR
```
### Building the Async VOL library
```bash
git clone https://github.com/hpc-io/vol-async.git
mkdir -p vol-async/build
cd vol-async/build
cmake .. -DCMAKE_INSTALL_PREFIX=$HDF5_VOL_DIR
make all install
```
Here, HDF5_VOL_DIR is set to be the prefix for installing all the vols. 

### Build the cache VOL library
```bash
git clone https://github.com/hpc-io/vol-cache.git
mkdir -p vol-cache/build
cd vol-cache/build
cmake .. -DCMAKE_INSTALL_PREFIX=$HDF5_VOL_DIR
make all install
```

To run the demo, set following environment variables first:
```bash
export HDF5_PLUGIN_PATH=$HDF5_VOL_DIR/lib
export HDF5_VOL_CONNECTOR="cache_ext config=config_1.cfg;under_vol=512;under_info={under_vol=0;under_info={}};"
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HDF5_ROOT/lib:$HDF5_PLUGIN_PATH
```
In this case, we have stacked Async VOL (VOL ID: 512) under the cache VOL to perform the data migration between the node-local storage and the global parallel file system. 

By default, the debugging mode is enabled to ensure the VOL connector is working. To disable it, simply remove the $(DEBUG) option from the CC line, and rerun make. 

All the setup of the local storage information is included in ```config_1.cfg```. Below is an example of config file
```config
HDF5_CACHE_STORAGE_SCOPE: LOCAL # the scope of the storage [LOCAL|GLOBAL]
HDF5_CACHE_STORAGE_PATH: /local/scratch # path of local storage
HDF5_CACHE_STORAGE_SIZE: 128188383838 # size of the storage space in bytes
HDF5_CACHE_STORAGE_TYPE: SSD # local storage type [SSD|BURST_BUFFER|MEMORY|GPU], default SSD
HDF5_CACHE_REPLACEMENT_POLICY: LRU # [LRU|LFU|FIFO|LIFO]
```

## Running the parallel HDF5 benchmarks
### Environment variables 
Currently, we use environmental variables to enable and disable the cache functionality. 
* HDF5_CACHE_RD [yes|no]: Whether to turn on caching for read. [default=no]
* HDF5_CACHE_WR [yes|no]: Whether to turn on caching for write. [default=no]

### Parallel write
* **write_cache.cpp** is the benchmark code for evaluating the parallel write performance. In this testing case, each MPI rank has a local
   buffer BI to be written into a HDF5 file organized in the following way: [B0|B1|B2|B3]|[B0|B1|B2|B3]|...|[B0|B1|B2|B3]. The repeatition of [B0|B1|B2|B3] is the number of iterations
   - --dim D1 D2: dimension of the 2D array [BI] // this is the local buffer size
   - --niter NITER: number of iterations. Notice that the data is accumulately written to the file. 
   - --scratch PATH: the location of the raw data
   - --sleep [seconds]: sleep between different iterations
   - --collective: whether to use collective I/O or not.
   
### Parallel read
* **prepare_dataset.cpp** this is to prepare the dataset for the parallel read benchark. 
```bash
mpirun -np 4 ./prepare_dataset --num_images 8192 --sz 224 --output images.h5
```
This will generate a hdf5 file, images.h5, which contains 8192 samples. Each 224x224x3 (image-base dataset)
* **read_cache.cpp, read_cache.py** is the benchmark code for evaluating the parallel read performance. We assume that the dataset is set us 
  - --input: HDF5 file [Default: images.h5]
  - --dataset: the name of the dataset in the HDF5 file [Default: dataset]
  - --num_epochs [Default: 2]: Number of epochs (at each epoch/iteration, we sweep through the dataset)
  - --num_batches [Default: 16]: Number of batches to read per epoch
  - --batch_size [Default: 32]: Number of samples per batch
  - --shuffle: Whether to shuffle the samples at the beginning of each epoch.
  - --local_storage [Default: ./]: The path of the local storage.

For the read benchmark, it is important to isolate the DRAM caching effect. By default, during the first iteration, the system will cache all the data on the memory (RSS), unless the memory capacity is not big enough to cache all the data. This ends up with a very high bandwidth at second iteration, and it is independent of where the node-local storage are.

To remove the cache / buffering effect for read benchmarks, one can allocate a big array that is close to the size of the RAM, so that it does not have any extra space to cache the input HDF5 file. This can be achieve by setting ```MEMORY_PER_PROC``` (memory per process in Giga Byte). **However, this might cause the compute node to crash.** The other way is to read dummpy files by seeting ```CACHE_NUM_FILES``` (number of dummpy files to read per process).
