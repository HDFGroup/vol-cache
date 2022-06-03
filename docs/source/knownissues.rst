Tested systems
================
So far we have tested Cache VOL on personal workstation, laptops (MAC and Linux), and DOE supercomputers such as Theta@ALCF, Summit@OLCF, Cori@NERSC. 

Known issues / limitations
==========================

Compiler issues
--------------
* On Summit@OLCF, Argobots does not work with PGI compiler. One has to use GNU compiler through "module load gcc".
* On Ubuntu Linux system, one has to manually add libabt.so to the LD_PRELOAD environment variable. 

Current limitations
--------------
* The current code only supports one direction workflows, either read only or write only. It does not support writing data to the caching storage layer and read it back immediately from there.
* For parallel read case, the prestaging / caching is done synchronously. We are in the process of implementing this feature. 

