Cache VOL APIs
==============
Beside using environment variable setup, the Cache VOL connector provides a set of functions for finer control of the asynchronous I/O operations. Application developers should be very careful with these APIs as they may cause unexpected behavior when not properly used. The "cache_new_h5api.h" header file must be included in the application's source code and the static async VOL library (libcache_new_h5api.a) must be linked.

* Enable Cache I/O for specific file

.. code-block::
    // set the file cache property list;
    // flag can be "HDF5_CACHE_WR", or "HDF5_CACHE_RD"
    herr_t H5Pset_fapl_cache(hid_t plist, flag, value)
    // Retrieve whether cache is enabled
    herr_t H5Pget_fapl_cache(hid_t plist, flag, value) # 

.. note::
    This enable the finer control of the cache effect for any specific file through the file access property list. The environment variable "HDF5_CACHE_WR" and "HDF5_CACHE_RD" will enable or disable the cache effect for all the files. In our design, the environment variable override the specific setting from the file access property list. 

* Pause/restart all async data migration operations.

.. code-block::

    // pause all the future async data migration 
    herr_t H5Fcache_async_op_pause(hid_t file_id);

    // Restart all paused operations, takes effect immediately.
    herr_t H5Fcache_async_op_start(hid_t file_id); 

