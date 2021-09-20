Preparation
===========

Some configuration parameters used in the instructions:

.. code-block::

    VOL_DIR : directory of HDF5 Cache VOL connector repository
    ABT_DIR : directory of Argobots source code
    HDF5_DIR  : directory of HDF5 source code


1. Download the Cache I/O VOL connector code (this repository) with Argobots git submodule Use system provided by HDF5.
Latest Argobots can also be downloaded separately from https://github.com/pmodels/argobots

.. code-block::

    git clone https://github.com/hpc-io/vol-cache.git

2. Download the HDF5 source code: currently, Cache VOL depends on a specific branch of HDF5. 

.. code-block::

    git clone -b post_open_fix https://github.com/hpc-io/hdf5.git

3. (Optional) Set the environment variables for the paths of the codes if the full path of VOL_DIR, ABT_DIR, and H5_DIR are not used in later setup.

.. code-block::

    export HDF5_DIR=/path/to/hdf5/dir
    export VOL_DIR=/path/to/async_vol/dir
    export ABT_DIR=/path/to/argobots/dir


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

Async VOL requires the setting of the following environmental variable to enable asynchronous I/O:

*Linux*

.. code-block::

    export LD_LIBRARY_PATH=$HDF5_VOL_DIR/lib:$ABT_DIR/lib:$LD_LIBRARY_PATH
    export HDF5_PLUGIN_PATH="$HDF5_VOL_DIR/lib"
    export HDF5_VOL_CONNECTOR="cache_ext config=conf1.dat;under_vol=512;under_info={under_vol=0;under_info={}}"

*MacOS*

.. code-block::

    export DYLD_LIBRARY_PATH=$HDF5_VOL_DIR/lib:$HDF5_ROOT/lib:$ABT_DIR/lib:$DYLD_LIBRARY_PATH
    export HDF5_PLUGIN_PATH="$HDF5_VOL_DIR/lib"
    export HDF5_VOL_CONNECTOR="cache_ext config=conf1.dat;under_vol=512;under_info={under_vol=0;under_info={}}"


In this case, we have stacked Async VOL (VOL ID: 512) under the cache VOL to perform the data migration between the node-local storage and the global parallel file system.

We assume that all the VOLs in the same folder HDF5_VOL_DIR/lib. By default, in the Makefile, we set the VOL folder HDF5_VOL_DIR to be $(HDF5_ROOT)/../vol/. The library files will be put in $(HDF5_ROOT)/../vol/lib/, and the header files will be put in $(HDF5_ROOT)/../vol/include. If you do not have write access to $(HDF5_ROOT)/../, please modify HDF5_VOL_DIR in ./src/Makefile.

By default, the debugging mode is enabled to ensure the VOL connector is working. To disable it, simply remove the $(DEBUG) option from the CC line, and rerun make.

All the setup of the local storage information is included in conf1.dat. Below is an example of config file

.. code-block::
   
    HDF5_CACHE_STORAGE_SCOPE LOCAL # the scope of the storage [LOCAL|GLOBAL], global storage is still not fully supported yet
    HDF5_CACHE_STORAGE_PATH /local/scratch # path of local storage
    HDF5_CACHE_STORAGE_SIZE 128188383838 # in unit of byte
    HDF5_CACHE_STORAGE_TYPE SSD # local storage type [SSD|BURST_BUFFER|MEMORY|GPU], default SSD
    HDF5_CACHE_REPLACEMENT_POLICY LRU # [LRU|LFU|FIFO|LIFO]
    
.. note::
    For some Linux systems, e.g. Ubuntu, LD_PRELOAD needs to be set to point to the shared libraries.

Test
====

1. Compile test codes

.. code-block::

    cd vol-cache/test
    make


2. Run tests

.. code-block::

    //Run serial and parallel tests
    sh run_test

    //Run the serial tests only
    sh run_test

3. Compile benchmark codes

.. code-block::

    cd vol-cache/benchmarks
    make 

4. Run benchmarks

.. code-block::

    HDF5_CACHE_WR=yes mpirun -np 2 ./test_write_cache
    HDF5_CACHE_RD=yes mpirun -np 2 ./test_read_cache

.. note::

   Please make sure the environment variables are set probably, and there is a configure file available in the current directory

Examples
=============

Please refer to the Makefile and source codes (test_*) under vol-cache/tests/ for example usage.

1. (Required) Set async VOL environment variables

See :ref:`Set Environmental Variables`

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


