Introduction
=============

Modern era high performance computing (HPC) systems are providing multiple levels of memory and storage layers to bridge the performance gap between fast memory and slow disk-based storage system managed by Lustre and GPFS. Several of the recent HPC systems are equipped with SSD and NVMe-based storage that is local on compute nodes. Some systems are providing an SSD-based ``burst buffer'' that is accessible by all compute nodes as a single file system. Although these hardware layers are intended to reduce the latency gap between memory and disk-based long-term storage, utilizing the fast layers has been left to the users. To our knowledge, fast storage layers have been often used as a scratch space local to a compute node and is rarely integrated into parallel I/O workflow. Cache VOL integrates node-local memory and storage, as well as global burst buffer storage layers as transparent caching or staging layers without placing the burden of managing these layers on users. It utilizes the Asynchronous HDF5 for migrate the data between different layers of storage, thus hiding most of parallel I/O overhead behind the compute.


.. image:: images/fast_storage_layer.png
	   

---------------------
High level Design
---------------------

'''''''''''''''''''''
Parallel write
'''''''''''''''''''''
In the parallel write case, the main idea is to stage the data to the fast storage and then moved tothe parallel file system asynchronously in the background. We treat the staging part differently for node-local storage and global storage:
1. For node-local storage, data is copied from the write buffer to the memory-mapped files using POSIX I/O. 
2. For global storage, data is write to a HDF5 file on the global storage layer. 
In the data migration part:
1. For node-local storage, data is transfered from the memory buffer (mapped to files on the node-local storage) to the parallel file system using HDF5 dataset write VOL function. 
2. For global storage, data is first read from the HDF5 file located at the global storage layer and then written to the HDF5 file on the parallel file system. 

.. image:: images/write.png

A few features: 	 
1. The data migration from the fast storage to the PFS is performed asynchronously in the background. This enables the application to hide the major I/O overhead behind the restparts of the application.
2. The dataset write call appears as a semi-blocking call. The H5Dwrite call will return after finishing writing datato the NLS. Therefore, the write buffers are immediately reusable for other purpose after the function call.
3. There is no extra memory allocation needed during the whole process. Data are staged in the memory-mapped files on the node-local storage. We use mmap pointers to address the data on the fast storage. 
4. Data is guaranteed to be flushed to the PFS at the end of the simulation. We wait for all the data migration tasks to finish before closing the dataset or the file at H5Dclose or H5Fclose. 

'''''''''''''''''''
Parallel read
'''''''''''''''''''
  
In the parallel read case, the main idea is to cache the dataset to the fast-storage layer first and then read them directly from the there for future requests.

For node-local storage, we divide the dataset into equal partitions, and predetermine where to cache each of the partitions. We use one-sided RMA for efficient remote data access. First, memory-mapped files are created on the node-local storage, one per process, each of size equal to the size of the dataset partition to be cached.We then associate the mmap pointer to a MPI Window. All the processes can then access data from remote nodes using RMA calls such as MPI_Put and MPI_Get.

.. image:: images/read.png

For global storage, we create another HDF5 file on the global storage, and data is cached to the global storage using HDF5 dataset write function. For any future read request, data will be read directly from the HDF5 file on the global storage. 




---------------------
Targeted workloads
---------------------
We expect our design will benefit the following two type of workloads: 

1. Intensive repetitive reading workloads, such as deep learning applications. In such workloads, the same dataset is being read again and again at each iteration, typically in a batch streaming fashion. The workloads are distributed in a data-parallel fashion. Using node-local storage to asynchronously stage the data into the node so that the application could directly read data from the node-local storage without going to the parallel file system. This will greatly improve the I/O performance and scaling efficiency. We expect various Deep learning based ECP projects, such as ExaLearn, CANDLE will benefit from this. 

2. Heavy check-pointing workloads. Simulations usually write intermediate data to the file system for the purpose of restarting or post-processing. Within our framework, the application will write the data to the node-local storage first and the data migration to the parallel file system is done in an async fashion without blocking the simulation. We expect this design will benefit those heavy check-pointing simulations, such as particle based dynamic simulation. ECP applications, such as Lammps, HACC will benefit from this. 
