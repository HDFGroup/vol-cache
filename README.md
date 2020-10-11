# Node local storage cache HDF5 VOL

This folder contains the prototype of system-aware HDF5 incoroprating node-local storage. This is part of the ExaHDF5 ECP project. 

Please find the the design document of the cache VOL in doc/.
## Files under the folder
### Source files under ./src
   * H5Dio_cache.c, H5Dio_cache.h -- source codes for incorporating node-local storage into parallel read and write HDF5. Including explicite cache APIs, and functions that are used for the cache VOL
   * H5VLcache_ext.c, H5VLcache_ext.h -- cache VOL, based on passthrough VOL connector
   
### Benchmark codes under ./benchmarks
   * test_write_cache.cpp -- testing code for parallel write
   * test_read_cache.cpp, test_read_cache.py -- benchmark code for parallel read
   
### Documentation under ./doc
   * node_local_storage_CCIO.tex -- prototype design based on explicit APIs and initial performance evaluation.
   * VOL design (in progress) is in [DESIGN.md](./doc/DESIGN.md). We also keep a copy in this [google document](https://docs.google.com/document/d/1j5WfMrkXJVe9mEx2kp-Yx6QeqNZNvqTERvtrOMRd-1w/edit?usp=sharing)

## Building the cache VOL

### HDF5 Dependency

This VOL depends on HDF5 cache branch. Currently, this branch has not been pushed back to the HDF5 github repo. It is located in my personal hdf5 fork repo.
```bash 
git clone -b cache https://github.com/zhenghh04/hdf5.git
```
**Note**: Make sure you have libhdf5 shared dynamic libraries in your hdf5/lib. For Linux, it's libhdf5.so, for OSX, it's libhdf5.dylib.

### Building HDF5 shared library
If you don't have the shared dynamic libraries, you'll need to reinstall HDF5.
- Get the latest version of the cache branch;
- In the repo directory, run ./autogen.sh
- In your build directory, run configure and make sure you **DO NOT** have the option "--disable-shared", for example:
```bash
./configure --prefix=H5_DIR/build --enable-parallel --enable-threadsafe --enable-unsupported CC=mpicc
make all install 
```

### Build the cache VOL library
Type *make* in the source dir and you'll see **libh5passthrough_vol.so**, which is the pass -hrough VOL connector library.
To run the demo, set following environment variables first:
```bash
export HDF5_PLUGIN_PATH=PATH_TO_YOUR_cache_vol
export HDF5_VOL_CONNECTOR="cache_ext under_vol=0;under_info={};"
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:PATH_TO_YOUR_hdf5_build/hdf5/lib:$HDF5_PLUGIN_PATH
```
By default, the debugging mode is enabled to ensure the VOL connector is working. To disable it, simply remove the $(DEBUG) option from the CC line, and rerun make. In the setup.sh file, we set

```bash
export HDF5_PLUGIN_PATH=$HDF5_ROOT/../vol/lib
export HDF5_VOL_CONNECTOR="cache_ext under_vol=0;under_info={};"
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${HDF5_ROOT}/lib:$HDF5_PLUGIN_PATH
```


## Running the parallel HDF5 benchmarks
### Environmental variables 
Currently, we use environmental variables to enable and disable the cache functionality. 
* HDF5_CACHE_RD/HDF5_CACHE_WR [yes|no]: Whether the cache functionality is turned on or not. [default=no]
* HDF5_LOCAL_STORAGE_PATH -- the path of the node local storage. 
* HDF5_LOCAL_STORAGE_SIZE -- size of the node local storage in unit of Giga Bytes. 

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

