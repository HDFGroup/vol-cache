#!/bin/sh
# This is for setup the environment

# 1) Setting HDF5 - the following module should set the HDF5-cache branch HDF5_ROOT

module load hdf5-cache

export HDF5_PLUGIN_PATH=$HDF5_ROOT/../vol/lib
export HDF5_VOL_CONNECTOR="cache_ext under_vol=0;under_info={};"
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HDF5_PLUGIN_PATH
