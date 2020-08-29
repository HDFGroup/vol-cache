# Design of node-local storage cache external VOL connector
* Huihuo Zheng (huihuo.zheng@anl.gov)
* Venkatram Vishwanath (venkat@anl.gov)
* Quincey Koziol (koziol@lbl.gov)
* Suren Byna (sbyna@lbl.gov)
* Haijun Tang (htang4@lbl.gov)
* Tony Lin (tonglinli@lbl.gov)

ChangeLog: 
* Aug 29, 2020 - Revisit the design
* July 7, 2020 - refine some function design
* June 23, 2020 - Included comments from Quincey and Suren including LLU and several changes of APIs
* June 8, 2020 - Initial draft design

Many hiph performance computing (HPC) systems have node- local storage attached to the compute nodes. We proposed an implementation to utilize the local storage as a cache to improve the parallel I/O performance of scientific simulation applications. We prototype the design in HDF5 using the external VOL connector framework. We name it as ```cache VOL```. This document outlines the major design in the ```cache VOL```. 


## Motivation
Many high performance computing (HPC) systems have two types of fast storage, a global parallel file system such as Lustre or GPFS and node-local storage attached to the compute nodes. To our knowledge, the node-local storage is rarely integrated into the parallel I/O workflows of real applications. 

We would like to use the node-local storage as a cache for the parallel file system to "effectively" improve the parallel I/O efficiency. Since node-local storage is attached to the compute nodes, writing/reading data to and from the node-local storage scale very well. We expect the aggregate I/O bandwidth to surpass the bandwidth of the parallel file system at large scale. Therefore, using node-local storage to cache/stage data will greatly benefit large scale I/O-heavy workloads. 

Specifically, we expect our design will benefit the following two types of workloads: 
* Intensive repetitive reading workloads, such as deep learning applications. In such workloads, the same dataset is being read again and again as the training proceeds, typically in a batch streaming fashion. The workloads are usually distributed in a data-parallel fashion. Using node-local storage to asynchronously stage the data into the node so that the application could directly read data from the node-local storage without going to the parallel file system, will greatly improve the I/O thoroughput as well as scaling efficiency. We expect various deep learning based ECP projects, such as ExaLearn, CANDLE will benefit from this. 

* Heavy check-pointing workloads. Simulations usually write intermediate data to the file system for the purpose of restarting or post-processing. Within our framework, the application can write the data to the node-local storage first and the data migration to the parallel file system is done in an async fashion without blocking the simulation. We expect this design will benefit those heavy check-pointing simulations, such as time series dynamic simulation. ECP applications, such as Lammps, HACC, E3SM, etc.

In order to make it easy for the application to adopt our implementation without much modification of their codes, we implement everything in the HDF5 Virtual Object Layer (VOL) framework. In the following, we outline the high level design of the cache VOL connector 

## High level API Design 

### Usage policy for the node-local storage
* We allow each file or dataset to claim a certain portion of the local storage for caching/staging its data.
* We create a unique folder associated with each file on the local storage and store all the cached data inside the folder. For dataset cache, we create a sub folder inside that file folder. 
* Properties of the cache
   - Purpose [```READ```/```WRITE```/```RDWR```] - what is the purpose of this cache, either cache for read workloads or write workloads, or read and write. Currently, we only focus on the first two modes, 
   
   Currently, for ```WRITE```, we simply make a copy of each write buffer to the node-local storage. We do not store enough metadata associate with the write buffer. Therefore, it is not possible to read back the data directly from the node-local storage. 
   
   For ```READ```, we prefetch the data into the node-local storage. A MPI window is created. The application read the data from the node-local storage using MPI_Get. This allows all the node to access data from other node's local storage. 
   
   ```RDWR``` will be supported in future. 
 
   - Duration [permanent/temporal]: We consider the capacity of the node-local storage is limited, and there might be multiple write / read trying to utilize it. Therefore, we define attribute a duration property associated with the cache. 
      - ```PERMANENT``` -  Permanent cache will exist throughout the entire simulation except being explicitly evicted. 
      - ```TEMPORAL``` - it will be released if other files use the space. By default, all the write cache will be temporal. Once the data is migrated to the parallel file system, the cache is automatically evicted, and the space is reusable by other files. 
   - Growable [true/false] whether we allow the size to grow or not. 
* We take account of each access to the cache. 
  - We document the following events: creation event, write / read events
  - The data eviction for the temporal cache is based on the access info according to certain algorithm to be specified
  - Once the data has been evicted from the node-local storage, the application has to go to the parallel file system to get the data.

### Public APIs 
These public APIs allow the application to have fine control on the node-local caches. In principle, If the application does not call these functions, we will have a set of default options for the application.

#### Local storage (LS) related functions
* H5LSset -- set the infomation of node local storage
 - the size
 - global path of the local storage 
 - such as /local/scratch (will interface with any system function calls)
* H5LSquery_XXX -- query the status of the local storage
 - path / namespace
 - space total
 - space left
 - list of files that are using the cache, and how much space they occupied

* H5LSregister_access_count -- registering any access events.

* H5LSclaim_space(size, char \*path, int opt, ...) -- claim certain space, if sucessfully, return the path. 
   * Allow soft claim or hard claim (soft claim will be the default claim option)
      - ```SOFT```: check on the space currently available
      - ```HARD```: will try to evict from the temporal cache if the space left is not enough
   * The application can specify what algorithm to use for hard claim. 
* H5LSrelease_space(char \*path) -- release the space

All these functions will be defined in ```H5LS.c``` and ```H5LS.h```. 

#### File related functions
* H5Fcache_create -- create a cache in the system’s local storage
* H5Fcache_remove -- remove the cache associated with the file in the system’s local storage (This will call H5LSremove_cache)
* H5Fset_cache_plist* - set the file cache property list; Set the cache property (space, etc) 
* H5Fget_cache_plist* / H5Fcache_query**- get the cache property list

* The ones denoted as * will be supported in future when the framework for extending property list within VOL are available. 


#### Dataset cache related functions [for read]
* H5Dcache_create -- reserve space for the data
* H5Dcache_remove -- clear the cache on the local storage related to the dataset
Besides these, we will also have the following two functions for prefetching / reading data from the cache
* H5Dprefetch -- pre-fetching the data from the file system and cache them to the local storage
* H5Dread_cache -- read data from the cache


### Environmental variables 
The following environmental variables set the path of the 
* ```LOCAL_STORAGE_PATH``` -- the path to the local storage, e.g., /local/scratch
* ```LOCAL_STORAGE_SIZE``` -- the size of the local storage in byte
* ```LOCAL_STORAGE_TYPE``` -- the type of local storage, [SSD|BURST_BUFFER|MEMORY]
* ```WRITE_CACHE_SIZE``` -- the default cache for write [1GB]

## References
1. Nicolae B, Moody A, Gonsiorowski E, Mohror K, Cappello F. VeloC: Towards High Performance Adaptive Asynchronous Checkpointing at Large Scale. In: 2019 IEEE International Parallel and Distributed Processing Symposium (IPDPS). ; 2019:911-920. doi:10.1109/IPDPS.2019.00099

2. Byna, S.; Breitenfeld, M. S.; Dong, B.; Koziol, Q.; Pourmal, E.; Robinson, D.; Soumagne, J.; Tang, H.; Vishwanath, V.; Warren, R. ExaHDF5: Delivering Efficient Parallel I/O on Exascale Computing Systems. Journal of Computer Science and Technology 2020, 35 (1), 145–160. https://doi.org/10.1007/s11390-020-9822-9.
