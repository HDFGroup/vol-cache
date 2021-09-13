Using Caches
===========
The HDF5 Cache I/O VOL connector (Cache VOL) allows applications to partially hide the I/O time from the computation. Here we provide more information on how applications can take advantage of it.

	
Data Consistency
----------------
For data write, in each of the H5Dwrite call, the data will be written to the fast storage layer first, and then migrated to the parallel file system in the background. Once the H5Dwrite returned, the buffer can be modified. 

For data read, after each H5Dread call, data is available in the buffer.

In other words, H5Dwrite and H5Dread behaves like synchronous calls. 


An Example
==========
An application may have the following HDF5 operations to write check point data:

.. code-block::

    // Synchronous file create at the beginning 
    fid = H5Fcreate(...);
    for (int iter=0; iter<niter; iter++) {
        // compute work
	...
        // Synchronous group create
        gid = H5Gcreate(fid, ...);
	// Synchronous dataset create
	did1 = H5Dcreate(gid, ..);
	did2 = H5Dcreate(gid, ..);
        // Synchronous dataset write
        status = H5Dwrite(did1, ..);
        // Synchronous dataset write again
        status = H5Dwrite(did2, ..);
        // close dataset
        err = H5Dclose(did1, ..);
        err = H5Dclose(did2, ..);
        // close group
        err = H5Gclose(gid, ..)
    }

    H5Fclose(fid);
    // Continue to computation

which can be converted to use async VOL as the following:

.. code-block::

       // Synchronous file create at the beginning 
    fid = H5Fcreate(...);
    for (int iter=0; iter<niter; iter++) {
        // compute work
	...
	// close the datasets & group after the next round of compute 
	if (iter > 0) {
	   H5Dclose(did1);
	   H5Dclose(did2);
	   H5Gclose(gid);
        }
        // Synchronous group create
        gid = H5Gcreate(fid, ...);
	// Synchronous dataset create
	did1 = H5Dcreate(gid, ..);
	did2 = H5Dcreate(gid, ..);
        // Synchronous dataset write
	// Pause data migration before issuing H5Dwrite calls
	H5Fcache_async_op_pause(fid);
        status = H5Dwrite(did1, ..);
        // Synchronous dataset write again
        status = H5Dwrite(did2, ..);
	// Start the data migration
	H5Fcache_async_op_start(fid);
        // close dataset
	if (iter==niter-1) {
	   err = H5Dclose(did1, ..);
           err = H5Dclose(did2, ..);
           // close group
           err = H5Gclose(gid, ..)
	}
    }

    H5Fclose(fid);


