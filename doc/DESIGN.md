# Design of node local storage cache external VOL connector
* Huihuo Zheng (huihuo.zheng@anl.gov)
* Venkatram Vishwanath (venkat@anl.gov)
* Quincey Koziol (koziol@lbl.gov)
* Suren Byna (sbyna@lbl.gov)
* Houjun Tang (htang4@lbl.gov)
* Tonglin Li (tonglinli@lbl.gov)

ChangeLog: 
* December 16, 2020 - separated out different modules for different types of storage [SSD|BURST_BUFFER|MEMORY]
* December 14, 2020 - added support for stacking multiple caching layers
* Sept 10, 2020 - added API functions with property list
* Aug 29, 2020 - Revisit the design
* July 7, 2020 - refine some function design
* June 23, 2020 - Included comments from Quincey and Suren including LLU and several changes of APIs
* June 8, 2020 - Initial draft design

Many hiph performance computing (HPC) systems have node-local storage attached to the compute nodes. We proposed an implementation to utilize the local storage as a cache to improve the parallel I/O performance of scientific simulation applications. We prototype the design in HDF5 using the external VOL connector framework. We name it as ```cache VOL```. This document outlines the major design in the ```cache VOL```. 

## Motivation
Many high performance computing (HPC) systems have two types of fast storage, a global parallel file system such as Lustre or GPFS and node-local storage attached to the compute nodes. To our knowledge, the node-local storage is rarely integrated into the parallel I/O workflows of real applications. 

We would like to use the node-local storage as a cache for the parallel file system to “effectively” improve the parallel I/O efficiency. In particular, since node-local storage is attached to the compute node, writing/reading data to and from the node-local storage will scale very well. At large scales, we expect the aggregate I/O bandwidth to surpass the bandwidth of the parallel file system. Therefore, using node-local storage to cache/stage data will greatly benefit large scale I/O-heavy workloads. 

Specifically, we expect our design will benefit the following two type of workloads: 

* Intensive repetitive reading workloads, such as deep learning applications. In such workloads, the same dataset is being read again and again at each iteration, typically in a batch streaming fashion. The workloads are distributed in a data-parallel fashion. Using node-local storage to asynchronously stage the data into the node so that the application could directly read data from the node-local storage without going to the parallel file system. This will greatly improve the I/O performance and scaling efficiency. We expect various Deep learning based ECP projects, such as ExaLearn, CANDLE will benefit from this. 

* Heavy check-pointing workloads. Simulations usually write intermediate data to the file system for the purpose of restarting or post-processing. Within our framework, the application will write the data to the node-local storage first and the data migration to the parallel file system is done in an async fashion without blocking the simulation. We expect this design will benefit those heavy check-pointing simulations, such as particle based dynamic simulation. ECP applications, such as Lammps, HACC will benefit from this. 


In order to make it easy for the application to adopt our implementation without much modification of their codes, we implement everything in the HDF5 Virtual Object Layer (VOL) framework. The purpose of this document is to outline the high level design of the cache VOL connector. 

## High level API Design 

### Policy of using the node-local storage
* We allow each file or dataset to claim a certain portion of the local storage for caching its data on the local storage.
* We create a unique folder associated with each file on the local storage and store all the cached data inside the folder. For dataset cache, we create a sub folder inside that file folder.  
* Properties of the cache
   - storage type [```SSD```/```BURST_BUFFER```/```MEMORY```]. Whether the node-local storage is SSD, burst buffer, or simply the memory. 
   - purpose [```READ```/```WRITE```/```RDWR```] - what is the purpose of this cache, either cache for read workloads or write workloads, or read and write. Currently, we only focus on the first two modes, ```RDWR``` will be supported in future. 
   - duration [permanent/temporal]
      - ```PERMANENT``` -  Permanent cache will exist throughout the entire simulation except being explicitly evicted.
      - ```TEMPORAL``` - it will be released if other files use the space. By default, all the write cache will be temporary. Once the data is migrated to the parallel file system, the cache is automatically evicted, and the space is reusable by other files.
   - growable [true/false] whether we allow the size to grow or not. 
* We take account of each access to the cache. 
  - We document the following events: creation event, write / read events
  - The data eviction for the temporal cache is based on the access info according to certain algorithm to be specified. We currently support LRU, LFU, and FIFO which could be specified through ```HDF5_CACHE_REPLACEMENT_POLICY```.
  - Once the data has been evicted from the node-local storage, the application has to go to the parallel file system to get the data.

### Public APIs 
These public APIs allow the application to have fine control on the node-local caches. In principle, If the application does not call these functions, we will have a set of default options for the application.

#### Local storage (LS) related functions
* H5Pcreate(H5P_LOCAL_STORAGE_CREATE) - creating a cache property list 
* H5LScreate(hid_t plist) - Creating a local storage struct from cache property list. 
* H5LSset -- set the infomation of node local storage
  - the size
  - global path of the local storage, such as /local/scratch (will interface with any system function calls)
* H5LSget() -- query the status of the local storage
  - path / namespace
  - space total
  - space left
  - list of files that are using the cache, and how much space they occupied
* H5LSclaim_space(LS, size, char \*path, int opt, ...) -- claim certain space, if sucessfully, return the path. 
   * Allow soft claim or hard claim (soft claim will be the default claim option)
      - ```SOFT```: check on the space currently available
      - ```HARD```: will try to evict from the temporal cache if the space left is not enough
   * The application can specify what algorithm to use for hard claim. 
* H5LSregister_cache (LS, cache) -- registering a cache to the cache list in the system’s local storage.
* H5LSremove_cache(LS, cache) -- remove a cache and associated files from the system’s local storage.
* H5LSremove_cache_all(LS, cache) -- remove all the caches and associated files from the system’s local storage.
* H5LSregister_access_count -- registering any access events.

All these functions will be defined in ```H5LS.c``` and ```H5LS.h```. 

We set up the local storage using a configuration file. The file name is passed through ```HDF5_VOL_CONNECTOR``` environment variable. In the configuration file, we set the following parameter: 
* ```HDF5_LOCAL_STORAGE_PATH``` -- the path to the local storage, e.g., /local/scratch
* ```HDF5_LOCAL_STORAGE_SIZE``` -- the size of the local storage in byte
* ```HDF5_LOCAL_STORAGE_TYPE``` -- the type of local storage, [SSD|BURST_BUFFER|MEMORY]
* ```HDF5_WRITE_CACHE_SIZE``` -- the default cache for write [default: 2GB] (noted that we do not have HDF5_READ_CACHE_SIZE because the read cache size will always be the size of the dataset to be cached)
* ```HDF5_CACHE_REPLACEMENT_POLICY``` -- cache replacement policy 
   - LRU - least recently used [default]
   - LFU - least frequently used 
   - FIFO - first in first out
   - LIFO - last in first out 

Below is an example config file: 
```
#contents of conf.dat
HDF5_LOCAL_STORAGE_PATH /local/scratch # path of local storage
HDF5_LOCAL_STORAGE_SIZE 128188383838 # in unit of byte
HDF5_LOCAL_STORAGE_TYPE SSD # local storage type [SSD|BURST_BUFFER|MEMORY], default SSD
HDF5_CACHE_REPLACEMENT_POLICY LRU # [LRU|LFU|FIFO|LIFO]
```

#### File cache related functions
* Set the cache property (space, etc) in file access property. Once this is set, cache will be created automatically when Fopen or Fcreate is called. 
   * H5Pset_fapl_cache(plist, flag, value) - set the file cache property list;
   * H5Pget_fapl_cache(plist, flag, value) - get the file cache property list.
   
   One can set ```HDF5_CACHE_RD``` / ```HDF5_CACHE_WR``` for independent files using H5Pset_fapl_cache()
   
* H5Fcache_create -- create a cache in the system’s local storage if it is not yet been created
* H5Fcache_remove -- remove the cache associated with the file in the system's local storage (This will call H5LSremove_cache)

#### Dataset cache related functions [for read]
* H5Dcache_create -- reserve space for the data
* H5Dcache_remove -- clear the cache on the local storage related to the dataset
Besides these, we will also have the following two functions for prefetching / reading data from the cache
* H5Dread_to_cache -- pre-fetching the data from the file system and cache them to the local storage
* H5Dread_from_cache -- read data from the cache


   
### Environment variables 
* ```HDF5_CACHE_RD``` -- whether to turn on cache for read [yes/no], [default: yes]
* ```HDF5_CACHE_WR``` -- whether to turn on cache for write [yes/no], [default: yes]

These environment variables will set the cache effort for all the files universally. If one want to control the caching effect from a file by file basis, one can set it to file access list using H5Pset_fapl_cache, and create / open the file using the property list. This will negate any global setting from the environment. 

### Stacking multiple caching VOL
One can setup mutiple caching VOL with each pointing to different tier of storage. For example, 
```bash
export HDF5_VOL_CONNECTOR="cache_ext config=conf1.dat;under_vol=518;under_info={config=conf2.dat;under_vol=0;under_info={}}"
```
In this case, the firt caching VOL is setup through `conf1.dat`, the second is through `conf2.dat`. The order of the caching location should be from higher to lower. 
