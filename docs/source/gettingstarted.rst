In this section, we present in details how to build and use Cache VOL. 

Preparation
===========

Some configuration parameters used in the instructions:

.. code-block::

    export HDF5_DIR=/path/to/hdf5/install/dir
    export HDF5_VOL_DIR=/path/to/vols/install/dir
    export ABT_DIR=/path/to/argobots/install/dir

We suggest the user to put all the VOL dynamic libraries into the same folder: HDF5_VOL_DIR.


1. Download the Cache VOL connector code (this repository) 

.. code-block::

    git clone https://github.com/hpc-io/vol-cache.git

2. Download the HDF5 source code: currently, Cache VOL works with the develop branch of HDF5. Some experimental features need the post_open_fix branch of HDF5. 

.. code-block::

    git clone -b develop https://github.com/HDFGroup/hdf5.git

.. note::     

  "GLOBAL" caching approach, caching as a single shared HDF5 file requires the post_open_fix branch of HDF5 which is available on https://github.com/hpc-io/hdf5.git. Please see "Experimental Features" section for more details. 


Installation
============

1. Compile HDF5

.. code-block::

    cd hdf5
    ./autogen.sh
    ./configure --prefix=$HDF5_DIR/ --enable-parallel --enable-threadsafe --enable-unsupported #(may need to add CC=cc or CC=mpicc)
    make && make install


2. Compile Argobots

.. code-block::

    git clone https://github.com/pmodels/argobots.git
    cd argobots
    ./autogen.sh  (may skip this step if the configure file exists)
    ./configure --prefix=$ABT_DIR/ #(may need to add CC=cc or CC=mpicc)
    make && make install


3. Compile Asynchronous VOL connector

.. code-block::

    cd ./src
    Edit "Makefile"
        Copy a sample Makefile (Makefile.cori, Makefile.summit, Makefile.macos), e.g. "cp Makefile.summit Makefile", which should work for most linux systems
        Change the path of HDF5_DIR and ABT_DIR to $H5_DIR/install and $ABT_DIR/install (replace $H5_DIR and $ABT_DIR with their full path)
        (Optional) update the compiler flag macros: DEBUG, CFLAGS, LIBS, ARFLAGS
        (Optional) comment/uncomment the correct DYNLDFLAGS & DYNLIB macros
    make
    cp lib* $HDF5_VOL_DIR/lib
    cp *.h $HDF5_VOL_DIR/include
    
Set Environment Variables
===========================

Async VOL and Cache VOL requires the setting of the following environment variables: 

*Linux*

.. code-block::

    export LD_LIBRARY_PATH=$HDF5_VOL_DIR/lib:$ABT_DIR/lib:$LD_LIBRARY_PATH
    export HDF5_PLUGIN_PATH="$HDF5_VOL_DIR/lib"
    export HDF5_VOL_CONNECTOR="cache_ext config=cache_1.cfg;under_vol=512;under_info={under_vol=0;under_info={}}"

For some Linux systems, e.g. Ubuntu, LD_PRELOAD needs to be set to point to the shared libraries.

 .. code-block::

    export LD_PRELOAD=$ABT_DIR/lib/libabt.so
    
*MacOS*

.. code-block::

    export DYLD_LIBRARY_PATH=$HDF5_VOL_DIR/lib:$HDF5_ROOT/lib:$ABT_DIR/lib:$DYLD_LIBRARY_PATH
    export HDF5_PLUGIN_PATH="$HDF5_VOL_DIR/lib"
    export HDF5_VOL_CONNECTOR="cache_ext config=cache_1.cfg;under_vol=512;under_info={under_vol=0;under_info={}}"

HDF5_VOL_CONNECTOR set the VOL connectors to be used according to specific order. In the case above, we have stacked Async VOL (VOL ID: 512) under Cache VOL to perform the data migration asynchronously. We use the 

We assume that all the VOLs are in the same folder HDF5_VOL_DIR. The library files will be put in $(HDF5_VOL_DIR/lib/, and the header files will be put in $(HDF5_VOL_DIR)//include. By default, in the Makefile, we set the VOL folder HDF5_VOL_DIR to be $(HDF5_ROOT)/../vol/. If you do not have write access to $(HDF5_ROOT)/../, please modify HDF5_VOL_DIR in ./src/Makefile.

By default, the debugging mode is enabled to ensure the VOL connector is working. To disable it, simply remove the $(DEBUG) option from the CC line, and rerun make.

All the setup of the local storage information is included in cache_1.cfg. Currently, we DO NOT yet support automatic detection of the cache storage. The user has to provide detail information manually. Below is an example of a config file

.. code-block::
   
    HDF5_CACHE_STORAGE_SCOPE: LOCAL # caching approach [LOCAL|GLOBAL] 
    HDF5_CACHE_STORAGE_PATH: /local/scratch # path of the storage for caching
    HDF5_CACHE_STORAGE_SIZE: 128188383838 # capacity of the storage in unit of byte
    HDF5_CACHE_WRITE_BUFFER_SIZE: 2147483648 # Storage space reserved for staging data to be written to the parallel file system. 
    HDF5_CACHE_STORAGE_TYPE: SSD # local storage type [SSD|BURST_BUFFER|MEMORY|GPU], default SSD
    HDF5_CACHE_REPLACEMENT_POLICY: LRU # [LRU|LFU|FIFO|LIFO]
    
.. note::

   Cache VOL will verify the existence of the storage path. If it does not exist, it will report error and abort the program.

   For parallel write case, a certain portion of space on each node-local storage (the size is specified by HDF5_CACHE_WRITE_BUFFER_SIZE*ppn, where ppn is the number of processes) is reserved for staging data from the write buffer. Please make sure that HDF5_CACHE_WRITE_BUFFER_SIZE*ppn is less than HDF5_CACHE_STORAGE_SIZE; otherwise, cache functionality will not be turned on. 

   For parallel read case, a certain protion of space of the size of the dataset will be reserved for each dataset. 

   For HDF5_CACHE_STORAGE_SCOPE=GLOBAL case, it is still experimental. Please see experimental features for details. 

Experimental Features
=====================
By default, Cache VOL works with both node-local storage and global storage. In both cases, the cache appears as one file per rank on the caching storage layer, if one sets "HDF5_CACHE_STORAGE_SCOPE" to be "LOCAL". However, for global storage layer, one can also cache data on a single shared HDF5 file by setting "HDF5_CACHE_STORAGE_SCOPE" to be "GLOBAL". The latter is still experimental. To enable this feature, one has to use the post_open_fix branch of HDF5 on https://github/hpc-io/hdf5. One also has to build the Cache VOL with "-DENABLE_GLOBAL_STORAGE_EXTENSION" flag 


Tests
======

There are two sets of tests provided. vol-cache/tests and vol-cache/benchmarks

1. Compile test codes

.. code-block::

    cd vol-cache/tests
    make
    cd - 
    cd vol-cache/benchmarks
    make
    cd -
    
2. Run tests

.. code-block::
   
    cd vol-cache/test
    sh run_test
    cd ../benchmarks/
    HDF5_CACHE_WR=yes mpirun -np 2 ./test_write_cache
    HDF5_CACHE_RD=yes mpirun -np 2 ./test_read_cache
    
.. note::

   Please make sure the environment variables are set properly, and there is a configure file available in the current directory

Examples
=============

Please refer to the Makefile and source codes (test_*) under vol-cache/tests/ for example usage.

1. (Required) Set async VOL environment variables

See :ref:`Set Environment Variables`

2. (Required) Init MPI with MPI_THREAD_MULTIPLE

Parallel HDF5 involve MPI collecive operations in many of its internal metadata operations, and they can be executed concurrently with the application's MPI operations, thus we require to initialize MPI with MPI_THREAD_MULTIPLE support. Change MPI_Init(argc, argv) in your application's code to:

.. code-block::

    MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided);

3. (Required) Postpone dataset close and group close calls after compute to allow overlap between data migration and compute. 

More detailed description on how to enable async VOL can be found in Hello Cache Section.

.. code-block::

    // Create event set for tracking async operations
    fid = H5Fcreate(..);
    did = H5Dopen();
    H5Dwrite(did, ...);
    // insert compute here. 
    ...
    H5Dclose(did, ...);
    H5Fclose(fid, ...);

4. (Optional) Include the header file if Cache VOL API is used (see Cache VOL APIs section)

   This allow finer controls such as enable caching only for specific files, paussing and restarting data migration if there is multiple consecutative H5Dwrite calls.
   
.. code-block::

    #include "cache_new_h5api.h" 
    ...
    H5Fcache_async_op_pause(fd);
    H5Dwrite()
    H5Dwrite()
    H5Dwrite()
    H5Fcache_async_op_start(fd);
    # Compute work to overlap with the data migration
    ...
