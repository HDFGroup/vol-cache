In this section, we present in details how to build and use Cache VOL. 

Preparation
===========

Some configuration parameters used in the instructions:

.. code-block::

    export HDF5_DIR=/path/to/hdf5/install/dir
    export HDF5_VOL_DIR=/path/to/vols/install/dir
    export ABT_DIR=/path/to/argobots/install/dir

We suggest the user to put all the VOL dynamic libraries (such as async, cache_ext, daos, etc) into the same folder: HDF5_VOL_DIR to allow stacking multiple connectors. 

Installation
============

1. Compile HDF5. Currently, Cache VOL works with the develop branch of HDF5 (with tag larger than 1.13.2). Two flags, --enable-parallel and --enable-threadsafe are needed for parallel I/O and multiple thread support. 

.. code-block::

    git clone -b develop https://github.com/HDFGroup/hdf5.git
    cd hdf5
    ./autogen.sh
    ./configure --prefix=$HDF5_DIR/ --enable-parallel \
        --enable-threadsafe --enable-unsupported #(may need to add CC=cc or CC=mpicc)
    make && make install


2. Compile Argobots

.. code-block::

    git clone https://github.com/pmodels/argobots.git
    cd argobots
    ./autogen.sh  #(may skip this step if the configure file exists)
    ./configure --prefix=$ABT_DIR/ #(may need to add CC=cc or CC=mpicc)
    make && make install


3. Compile Asynchronous VOL connector

.. code-block::

    git clone https://github.com/HDFGroup/vol-async.git
    cd vol-async
    mkdir build
    cd build
    cmake .. -DCMAKE_INSTALL_PREFIX=$HDF5_VOL_DIR
    make all install
    
    
4. Compile Cache VOL connector

.. code-block::

    git clone https://github.com/HDFGroup/vol-cache.git
    cd vol-cache
    mkdir build
    cd build
    cmake .. -DCMAKE_INSTALL_PREFIX=$HDF5_VOL_DIR
    make all install


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

HDF5_VOL_CONNECTOR set the VOL connectors to be used according to specific order. In the case above, we have stacked Async VOL (VOL ID: 512) under Cache VOL to perform data migration asynchronously. The native VOL is used as the final terminal VOL connector to talk directly to the permanent storage.

We assume that all the VOLs are in the same folder ${HDF5_VOL_DIR}. The library files will be put in ${HDF5_VOL_DIR}/lib/, and the header files will be put in ${HDF5_VOL_DIR}/include. By default, in the Makefile, we set the VOL folder HDF5_VOL_DIR to be ${HDF5_ROOT}/../vol/. If you do not have write access to ${HDF5_ROOT}/../, please modify HDF5_VOL_DIR variable in ./src/Makefile.

By default, the debugging mode is enabled to ensure the VOL connector is working. To disable it, simply remove the $(DEBUG) option from the CC line, and rerun make.

All the setup of the local storage information is included in cache_1.cfg. Currently, we DO NOT yet support automatic detection of the node-local storage. The user has to provide detailed information manually. Below is an example of a config file

.. code-block::
   
    HDF5_CACHE_STORAGE_SCOPE: LOCAL # caching approach [LOCAL|GLOBAL] 
    HDF5_CACHE_STORAGE_PATH: /local/scratch # path of the storage for caching
    HDF5_CACHE_STORAGE_SIZE: 128188383838 # capacity of the storage in unit of byte
    HDF5_CACHE_WRITE_BUFFER_SIZE: 2147483648 # Storage space reserved for staging data to be written to the parallel file system. 
    HDF5_CACHE_STORAGE_TYPE: SSD # local storage type [SSD|BURST_BUFFER|MEMORY|GPU], default SSD
    HDF5_CACHE_REPLACEMENT_POLICY: LRU # [LRU|LFU|FIFO|LIFO]
    HDF5_CACHE_FUSION_THRESHOLD: 16777216 # Threshold beyond which the data is flushed to the terminal storage layer. This only works for HDF5 version > 1.13.3. 
    
.. note::

   Cache VOL will verify the existence of the storage path. If the namespace provided is not accessible, it will report error and abort the program.

   For parallel write case, a certain portion of space on each node-local storage (the size is specified by HDF5_CACHE_WRITE_BUFFER_SIZE*ppn, where ppn is the number of processes) is reserved for staging data from the write buffer. Please make sure that HDF5_CACHE_WRITE_BUFFER_SIZE*ppn is less than HDF5_CACHE_STORAGE_SIZE; otherwise, cache functionality will not be turned on. 

   For parallel read case, a certain protion of space of the size of the dataset will be reserved for each dataset. 
   
   By default, Cache VOL works with both node-local storage and global storage. In both cases, the cache appears as one file per rank on the caching storage layer, if one sets "HDF5_CACHE_STORAGE_SCOPE" to be "LOCAL". However, for global storage layer, one can also cache data on a single shared HDF5 file by setting "HDF5_CACHE_STORAGE_SCOPE" to be "GLOBAL". 


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
    ./prepare_datasets  #this will prepare images.h5 for the test_read_cache test.
    HDF5_CACHE_RD=yes mpirun -np 2 ./test_read_cache
    
.. note::

   Please make sure the environment variables are set properly, and there is a configure file available in the current directory

Examples
=============

Please refer to the Makefile and source codes (test_*) under vol-cache/tests/ for example usage.

1. (Required) Set Cache VOL environment variables

See :ref:`Set Environment Variables`

2. (Required) Enable Cache VOL by setting HDF5_CACHE_WR and HDF5_CACHE_RD to yes. This will turn on caching for all the files. One can also enable caching only for some specific file by setting the file access property list.

.. code-block::

    herr_t H5Pset_fapl_cache(hid_t plist, "HDF5_CACHE_WR", true);


2. (Required) Init MPI with MPI_THREAD_MULTIPLE

Parallel HDF5 involve MPI collecive operations in many of its internal metadata operations, and they can be executed concurrently with the application's MPI operations, thus we require to initialize MPI with MPI_THREAD_MULTIPLE support. Change MPI_Init(argc, argv) in your application's code to:

.. code-block::

    MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided);

3. (Required) Postpone dataset close and group close calls after compute to allow overlap between data migration and compute. Two API functions are created for this purpose. One can also set "HDF5_CACHE_DELAY_CLOSE" to yes to achieve the same purpose without changing the code. 

.. code-block::

    // Create event set for tracking async operations
    fid = H5Fcreate(..);
    did = H5Dopen();
    H5Dwrite(did, ...);
    // insert compute here. 
    ...
    H5Dclose(did, ...);
    H5Fclose(fid, ...);

4. (Optional) Include the header file "cache_new_h5api.h" if Cache VOL API is used (see Cache VOL APIs section)

   This allow finer controls such as enable caching only for specific files, paussing and restarting data migration if there is multiple consecutative H5Dwrite calls.
   
.. code-block::

    #include "cache_new_h5api.h" 
    ...
    H5Fcache_async_op_pause(fd); // stop asynchronous data migration tasks 
    H5Dwrite()
    H5Dwrite()
    H5Dwrite()
    H5Fcache_async_op_start(fd); // start all the asynchronous data migration tasks all at once.
    # Compute work to overlap with the data migration
    ...
