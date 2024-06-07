.. HDF5 Cache I/O VOL Connector documentation master file

HDF5 Cache VOL Connector
===============================================================

As the scientific computing enters in the to exascale and big dataera, the amount of data produced by the simulation is significantly increased. Meanwhile, data analytics and artificial intelligence have risen up to become two important pillars in scientific computing, both of which are data intensive workloads. Therefore, being able to store or load data efficiently to and from the storage system becomes increasingly important to scientific computing. On the hardware level, many pre-exascale and exascale systems are designed to be equipped with fast storage layer in between the compute node memory and the parallel file system. Examples include burst buffer NVMes SSDs on Summit, Theta, Polaris and the upcoming Frontier system. It is a challenging problem to effectively incorparate these fast storage layer to improve the parallel I/O performance.

We design a HDF5 Cache Virtual Object Layer (VOL) connector that provides support for reading and writing data directly from / to the fast storage layer, while performing the data migration between the fast storage layer and permanent global parallel file system in the background, to allow hiding majority of the I/O overhead behind the computation of the application. The VOL framework provides an easy-to-use programming interface for the application to adapt.

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
   :caption: Tested systems and Known issues

   knownissues
   
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
