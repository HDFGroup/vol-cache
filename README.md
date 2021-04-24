# Node local storage cache HDF5 VOL

This folder contains the prototype of caching vol. This is part of the ExaHDF5 ECP project. 

Please find the the design document of the cache VOL in ./doc/
## Files under the folder
### Source files under ./src
   * cache_utils.c, cache_utils.h --  utility functions that are used for the cache VOL
   * H5VLcache_ext.c, H5VLcache_ext.h -- cache VOL, based on passthrough VOL connector
   * H5LS.c, H5LS.h -- functions for managing cache storage
   * cache_new_h5api.h, cache_new_h5api.c -- new public API within the scope of cache VOL. 
   
### Benchmark codes under ./benchmarks
   * test_write_cache.cpp -- testing code for parallel write
   * test_read_cache.cpp, test_read_cache.py -- benchmark code for parallel read
   
### Documentation under ./doc
   * cache_vol.tex -- prototype design based on explicit APIs and initial performance evaluation.
   * VOL design (in progress) is in [DESIGN.md](./doc/DESIGN.md).

## Building the Cache VOL
### HDF5 Dependency

This caching VOL connector depends on a particular HDF5 branch. Currently, this branch has not been pushed back to the HDF5 github repo. 
```bash 
git clone -b async_vol_register_optional https://github.com/hpc-io/hdf5.git
```

### Building HDF5 shared library
Make sure you have libhdf5 shared dynamic libraries in your hdf5/lib. For Linux, it's libhdf5.so, for OSX, it's libhdf5.dylib. If you don't have the shared dynamic libraries, you'll need to reinstall HDF5.
- Get the latest version of the cache branch;
- In the repo directory, run ./autogen.sh
- In your build directory, run configure and make sure you **DO NOT** have the option "--disable-shared", for example:
```bash
./configure --prefix=H5_DIR/build --enable-parallel --enable-threadsafe --enable-unsupported CC=mpicc
make all install 
```

### Building the Async VOL library
The benchmark codes depend on the Async VOL. Please follow the instruction to install the Async VOL: https://github.com/hpc-io/vol-async (async_vol_register_optional branch). 

### Build the caching VOL library
Type *make* in the source dir and you'll see **libh5cache_vol.so**, which is the pass -hrough VOL connector library.
To run the demo, set following environment variables first:
```bash
export HDF5_PLUGIN_PATH=PATH_TO_YOUR_cache_vol
export HDF5_VOL_CONNECTOR="cache_ext config=config1.dat;under_vol=0;under_info={};"
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:PATH_TO_YOUR_hdf5_build/hdf5/lib:$HDF5_PLUGIN_PATH
```

Please all the VOLs in the same folder. By default, in the Makefile, we set the VOL folder to be $(HDF5_ROOT)/../vol/. The library files will be put in $(HDF5_ROOT)/../vol/lib/, and the header files will be put in $(HDF5_ROOT)/../vol/include. If you do not have write access to $(HDF5_ROOT)/../, please modify ```HDF5_VOL``` in ./src/Makefile.  

By default, the debugging mode is enabled to ensure the VOL connector is working. To disable it, simply remove the $(DEBUG) option from the CC line, and rerun make. 

All the setup of the local storage information is included in ```conf1.dat```. Below is an example of config file
```config
HDF5_CACHE_STORAGE_SCOPE LOCAL # the scope of the storage [LOCAL|GLOBAL], global storage is still not fully supported yet
HDF5_CACHE_STORAGE_PATH /local/scratch # path of local storage
HDF5_CACHE_STORAGE_SIZE 128188383838 # in unit of byte
HDF5_CACHE_STORAGE_TYPE SSD # local storage type [SSD|BURST_BUFFER|MEMORY], default SSD
HDF5_CACHE_REPLACEMENT_POLICY LRU # [LRU|LFU|FIFO|LIFO]
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

For this benchmark, it is important to isolate the cache effect. By default, during the first iteration, the system will cache all the data on the memory (RSS), unless the memory capacity is not big enough to cache all the data. This ends up with a very high bandwidth at second iteration, and it is independent of where the node-local storage are.

To remove the cache / buffering effect for read benchmarks, one can allocate a big array that is close to the size of the RAM, so that it does not have any extra space to cache the input HDF5 file. This can be achieve by setting ```MEMORY_PER_PROC``` (memory per process in Giga Byte). **However, this might cause the compute node to crash.** The other way is to read dummpy files by seeting ```CACHE_NUM_FILES``` (number of dummpy files to read per process).