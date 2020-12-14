#!/bin/sh
# This is for setup the environment

# 1) Setting HDF5 - the following module should set the HDF5-cache branch HDF5_ROOT
module load hdf5-cache 

# 2) setting VOL path
export HDF5_PLUGIN_PATH=$HDF5_ROOT/../vol/lib
<<<<<<< HEAD
#export HDF5_VOL_CONNECTOR="passthru_ext under_vol=707;under_info={under_vol=0;under_info={}}"
export HDF5_VOL_CONNECTOR="passthru_ext under_vol=707;under_info={under_vol=0;under_info={}}"
#export HDF5_VOL_CONNECTOR="cache_ext under_vol=0;under_info={}"
=======
export HDF5_VOL_CONNECTOR="cache_ext under_vol=0;under_info={};"
>>>>>>> fd0c996a40448af76e2ba9104c0dde01a3c962ff
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HDF5_PLUGIN_PATH
export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:$HDF5_PLUGIN_PATH

# 3) Setting environmental variables for cache VOL
# HDF5_CACHE_RD=yes
# HDF5_CACHE_WR=not
# HDF5_LOCAL_STORAGE_PATH=/local/scratch
# HDF5_LOCAL_STORAGE_SIZE=128
# HDF5_LOCAL_STORAGE_TYPE=SSD
