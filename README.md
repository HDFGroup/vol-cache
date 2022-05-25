# Node-local storage cache HDF5 VOL

Documentation: https://vol-cache.readthedocs.io

This folder contains cache VOL, which is a part of the ExaHDF5 ECP project. The main objective of the Cache VOL is to incorporate fast storage layers (e.g, burst buffer, node-local storage) into parallel I/O workflow for caching and staging data to improve the I/O efficiency at large scale. 

## Files under this folder
* ./src - Cache VOL source files
   * cache_utils.c, cache_utils.h --  utility functions
   * H5VLcache_ext.c, H5VLcache_ext.h -- cache VOL
   * H5LS.c, H5LS.h -- functions for managing cache storage
   * cache_new_h5api.h, cache_new_h5api.c -- new public API functions specific to the cache VOL. 
   
* ./benchmarks - microbenchmark codes
   * test_write_cache.cpp -- testing code for parallel write
   * test_read_cache.cpp, test_read_cache.py -- benchmark code for parallel read
   
* Documentation under ./doc
   * cache_vol.tex -- prototype design based on explicit APIs and initial performance evaluation.
   * readthedoc -- https://vol-cache.readthedocs.io
* tests: this contains a set of tests for different functional. 

## Building the Cache VOL
### Building HDF5 shared library
Currently, the cache VOL depends on the develop branch of HDF5, 
```bash 
git clone -b develop https://github.com/HDFGroup/hdf5.git
cd hdf5
./autogen.sh
./configure --prefix=HDF5_ROOT --enable-parallel --enable-threadsafe --enable-unsupported CC=mpicc
make all install 
```
When running configure, ake sure you **DO NOT** have the option "--disable-shared". 

### Building the Async VOL library
```bash
git clone https://github.com/hpc-io/vol-async.git
cd vol-async
mkdir build
cd build
cmake .. -DCMAKE_INSTALL_PREFIX=$HDF5_VOL_DIR
make all install
```

### Build the cache VOL library
```bash
git clone https://github.com/hpc-io/vol-cache.git
cd vol-cache
mkdir build
cd build
cmake .. -DCMAKE_INSTALL_PREFIX=$HDF5_VOL_DIR
make all install
```

To run the demo, set following environment variables first:
```bash
export HDF5_PLUGIN_PATH=HDF5_VOL_DIR/lib
export HDF5_VOL_CONNECTOR="cache_ext config=config1.dat;under_vol=512;under_info={under_vol=0;under_info={}};"
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HDF5_ROOT/lib:$HDF5_PLUGIN_PATH
```
In this case, we have stacked Async VOL (VOL ID: 512) under the cache VOL to perform the data migration between the node-local storage and the global parallel file system. 

We assume that all the VOLs in the same folder ```HDF5_VOL_DIR/lib```. By default, in the Makefile, we set the VOL folder ```HDF5_VOL_DIR``` to be ```$(HDF5_ROOT)/../vol/```. The library files will be put in ```$(HDF5_ROOT)/../vol/lib/```, and the header files will be put in ```$(HDF5_ROOT)/../vol/include```. If you do not have write access to $(HDF5_ROOT)/../, please modify ```HDF5_VOL_DIR``` in ./src/Makefile.  

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
* HDF5_CACHE_RD [yes|no]: Whether the cache functionality is turned on or not for read. [default=no]
* HDF5_CACHE_WR [yes|no]: Whether the cache functionality is turned on or not for write. [default=no]

### Parallel write
* **test_write_cache.cpp** is the benchmark code for evaluating the parallel write performance. In this testing case, each MPI rank has a local
   buffer BI to be written into a HDF5 file organized in the following way: [B0|B1|B2|B3]|[B0|B1|B2|B3]|...|[B0|B1|B2|B3]. The repeatition of [B0|B1|B2|B3] is the number of iterations
   - --dim D1 D2: dimension of the 2D array [BI] // this is the local buffer size
   - --niter NITER: number of iterations. Notice that the data is accumulately written to the file. 
   - --scratch PATH: the location of the raw data
   - --sleep [secons]: sleep between different iterations
   - --collective: whether to use collective I/O or not.
   
### Parallel read
* **prepare_dataset.cpp** this is to prepare the dataset for the parallel read benchark. 
```bash
mpirun -np 4 ./prepare_dataset --num_images 8192 --sz 224 --output images.h5
```
This will generate a hdf5 file, images.h5, which contains 8192 samples. Each 224x224x3 (image-base dataset)
* **test_read_cache.cpp, test_read_cache.py** is the benchmark code for evaluating the parallel read performance. We assume that the dataset is set us 
  - --input: HDF5 file [Default: images.h5]
  - --dataset: the name of the dataset in the HDF5 file [Default: dataset]
  - --num_epochs [Default: 2]: Number of epochs (at each epoch/iteration, we sweep through the dataset)
  - --num_batches [Default: 16]: Number of batches to read per epoch
  - --batch_size [Default: 32]: Number of samples per batch
  - --shuffle: Whether to shuffle the samples at the beginning of each epoch.
  - --local_storage [Default: ./]: The path of the local storage.

For the read benchmark, it is important to isolate the DRAM caching effect. By default, during the first iteration, the system will cache all the data on the memory (RSS), unless the memory capacity is not big enough to cache all the data. This ends up with a very high bandwidth at second iteration, and it is independent of where the node-local storage are.

To remove the cache / buffering effect for read benchmarks, one can allocate a big array that is close to the size of the RAM, so that it does not have any extra space to cache the input HDF5 file. This can be achieve by setting ```MEMORY_PER_PROC``` (memory per process in Giga Byte). **However, this might cause the compute node to crash.** The other way is to read dummpy files by seeting ```CACHE_NUM_FILES``` (number of dummpy files to read per process).
