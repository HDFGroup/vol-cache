Introduction
=============

Modern era high performance computing (HPC) systems are providing multiple levels of memory and storage layers to bridge the performance gap between fast memory and slow disk-based storage system managed by global parallel file system such as Lustre and GPFS. Several of the recent HPC systems are equipped with SSD and NVMe-based storage that is locally attached to compute nodes. Some systems are providing an SSD-based ``burst buffer'' that is accessible by all compute nodes as a ``global" file system, such as the burst buffer on Cori supercomputer at NERSC. Although these hardware layers are intended to reduce the latency gap between memory and disk-based long-term storage, utilizing the fast layers has been left to the users. To our knowledge, fast storage layers have been often used as a scratch space as slow a ``memory" extenstion to the compute node RAM. They are rarely integrated into parallel I/O workflow.

.. image:: images/fast_storage_layer.png

The goal of the Cache VOL is to transparently manage the fast storage layers inside the library to improve the overall parallel I/O performance. Cache VOL integrates node-local memory and storage, as well as global burst buffer storage layers as transparent caching or staging layers inside the I/O library, without placing the burden of managing these layers on users. It can be stacked together with Async VOL to utilizes asynchronous HDF5 operation to transfer data between different layers of storage in the background, thus hiding most of parallel I/O overhead behind the compute. We suggest the users to always stack Cache VOL with Async VOL. 


---------------------
High level Design
---------------------

In Cache VOL, we treat the read and write differently. 

'''''''''''''''''''''
Parallel write
'''''''''''''''''''''
In the parallel write case, the main idea is to stage the data to the fast storage and then moved to the parallel file system asynchronously in the background. In other word, the application write the data to the fast storage layers, and the library take care of the data migration in the background while the application continue to proceed to other part of the simulation. 

1. For node-local caching storage, data held by the write buffer is written to memory-mapped files on the node-local storage throught POSIX write. The function call returns right the way after the data being written. 

2. For global caching storage, a HDF5 file is created on the global storage layer for temperally staging the data. Data is then written to the HDF5 file on the global storage layer. 

In the data migration part:

1. For node-local storage, data is transfered from the node-local storage to the parallel file system using dataset write function from the under VOL. Not extra memory allocation is needed in this case since a pointer is used to address the data on the memory mapped file. 

2. For global storage, data is first read from the HDF5 file located at the global storage layer and then written to the HDF5 file on the parallel file system. In this case, we dynamically allocate a buffer to temporally hold the data. We free the buffer after the data has been transfered to the parallel file system. 

Moving data from the fast storage layer to the global parallel file system is performed through dataset write function from the under below the Cache VOL. If Async VOL is stacked below the Cache VOL, then the migration will be done asynchronously, which will be the suggested way of using Cache VOL. 

.. image:: images/write.png

A few features summarized as follows: 	 

1. The data migration from the fast storage to the PFS is performed asynchronously in the background through Async VOL. This enables the application to hide majority of the I/O overhead behind the rest parts of the application. Therefore, it is suggested to insert some compute work right after the dataset write function before closing the dataset. 

2. The dataset write call appears as a semi-blocking call. The H5Dwrite call will return right after the data has been written to caching storage layer. Therefore, the write buffers are immediately reusable for other purpose after the H5Dwrite call. 

3. There is no extra memory allocation needed during the whole process. Data are staged in the memory-mapped files on the node-local storage. We use mmap pointers to address the data on the fast storage. 

4. Data is guaranteed to be flushed to the parallel file system at the end of the simulation. We wait for all the data migration tasks to finish before closing the dataset or the file at H5Dclose or H5Fclose. 

'''''''''''''''''''
Parallel read
'''''''''''''''''''
  
In the parallel read case, the main idea is to stage data to the fast-storage layer first and then read them directly from the there for future requests.

For node-local storage, we divide the dataset into equal partitions, and predetermine where to cache each of the partitions. We use one-sided RMA for efficient remote data access. First, memory-mapped files are created on the node-local storage, one per process, each of size equal to the size of the partition to be cached. We then associate the mmap pointer to a MPI Window to expose a portion of the storage to other processes. All the processes can then access data from remote nodes using RMA calls such as MPI_Put and MPI_Get.

.. image:: images/read.png

For global storage, we create another HDF5 file on the global storage, and data is cached to the global storage using HDF5 dataset write function from native dataset VOL. For any future read request, data will be read directly from the HDF5 file on the global storage. 

---------------------
Targetting workloads
---------------------
We expect the Cache VOL will benefit the following two type of workloads: 

1. Intensive repetitive reading workloads, such as deep learning applications. In such workloads, the same dataset is being read again and again at each iteration, typically in a batch streaming fashion. The workloads are distributed in a data-parallel fashion. Using node-local storage to asynchronously stage the data into the node so that the application could directly read data from the node-local storage without going to the parallel file system. This will greatly improve the I/O performance and scaling efficiency. We expect various Deep learning based ECP projects, such as ExaLearn, CANDLE will benefit from this. 

2. Heavy check-pointing workloads. Simulations usually write intermediate data to the file system for the purpose of restarting or post-processing. Within our framework, the application will write the data to the node-local storage first and the data migration to the parallel file system is done in an async fashion without blocking the simulation. We expect this design will benefit those heavy check-pointing simulations, such as particle based dynamic simulation. ECP applications, such as Lammps, HACC will benefit from this. 
