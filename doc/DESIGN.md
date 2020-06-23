# Design of node local storage cache external VOL connector
Date: June 8, 2020

## Motivation - targeting workloads
* Parallel read - Deep learning workloads: repeatedly read with Data parallel training (require good scaling)
* Parallel write - heavy check-pointing workloads

## Design 

### Policy of using the node local storage
* We allow each **file** to claim certain portion of the local storage for caching its data on the local storage. 
* We create a unique folder associated with each file on the local storage and store all the cached data inside the folder. 
* Properties of the cache
   - purpose [```READ```/```WRITE```] - what is the purpose of this cache, either cache for read workloads or write worklads.
   - duration [permanent/temporal]
      - ```PERMANENT``` - Permanent cache will exist through out the entire simulation except being explicitly evicted. 
      - ```TEMPORAL``` - it will be released if other file use the space. By default, all the write cache will be temporal. Once the data is migrated to the parallel file system, the cache is automatically evicted, and the space is reusable by other file. 
   - growable [true/false] whether we allow the size to grow or not. 
* We take account of each access to the cache. 
   - We document the following events: creation event, write / read events
   - The data eviction for the temporal cache is based on the acess info according to certain algorithm to be specified
   - Once the data has been evicted from the node-local storage, the application has to go to the parallel file system to get the data. 

```
struct LS_info {
   type                     # memory, SSD, burst buffer, etc
   namespace                # the namespace for the local storage
   space_total              # the size of the local storage
   space_left               # the space left on the local storage
   file_list {path, size, access_count} 
                            # a list of files that are currently have data cached on the local strorage
}
```

### Public APIs 
These public APIs allow the application to have fine control on the node-local caches. In principle, If the application do not call these functions, we will have a set of default options for the application. 

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
* H5Fset_cache_plist - set the file cache property list
  * Set the cache property (space, etc)
* H5Fget_cache_plist - get the cache property list
* H5Fquery_cache - querey information related with the cache from the property list (space, path, type of cache, purpose of cache, etc)
* H5Fclear_cache - clear all the cache on SSD, and then call H5LSrelease_space()

#### Dataset related
* H5Dreserve_cache -- reserve space for the data
* H5Dclear_cache -- clear the cache on the local storage related to the dataset

Besides these, we will also have the following two functions for prefetching / reading data from the cache
* H5Dprefetch -- prefetching the data from the file system and cache them to the local storage
* H5Dread_cache -- read data from the cache