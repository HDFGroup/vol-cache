.. HDF5 Cache I/O VOL Connector documentation master file

Cache HDF5 VOL Connector
===============================================================

As the scientific computing enters in the to exascale era, the amount of data produced by the simulation is significantly increased. Being able to storing or loading data efficiently to the storage system thus become increasingly important. Many pre-exascale and exascale systems are designed to be equiped with fast storage layer in between the compute node memory and the parallel file system. Examples include burst buffer NVMe on Summit and Theta, and in the upcoming Frontier system. It is a challenging problem to effectively utilize these fast storage layer to improve the parallel I/O performance.

We present an cache I/O framework that provides support for reading and writing data directly from / to the fast storage layer, while performing the data migration bewteen the fast storage layer and parallel file system in the background. 

Our prototype Cache I/O implementation as an HDF5 VOL connector demonstrates the effectiveness of hiding the I/O cost from the application with low overhead and easy-to-use programming interface.

.. toctree::
   :maxdepth: 2
   :caption: Overview

   overview

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   gettingstarted

.. toctree::
   :maxdepth: 2
   :caption: Public API

   cacheapi

.. toctree::
   :maxdepth: 2
   :caption: Best Practices

   bestpractices

.. toctree::
   :maxdepth: 2
   :caption: Legal

   copyright
   license


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
