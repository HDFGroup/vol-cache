#!/usr/bin/env python
# This is still under development...
import os, sys
ld_library_path = os.environ["LD_LIBRARY_PATH"]+os.environ["DYLD_LIBRARY_PATH"]

def setup_env():
    os.system("stack_vol.py cache_ext async")
    os.environ['HDF5_PLUGIN_PATH']=os.environ['HDF5_VOL_DIR']
    os.environ['HDF5_VOL_CONNECTOR'] = "cache_ext config=conf1.dat;under_vol=512;under_info={under_vol=0;under_info={}}"
    os.environ["DYLD_LIBRARY_PATH"] = os.environ["DYLD_LIBRARY_PATH"] + ":"+os.environ['HDF5_PLUGIN_PATH']
    os.environ["LD_LIBRARY_PATH"] = os.environ["LD_LIBRARY_PATH"] + ":"+os.environ['HDF5_PLUGIN_PATH']

#def test_cache_vol_env():
#    assert(ld_library_path.find("libcache_vol")!=-1)

#def test_async_vol_env():
#    assert(ld_library_path.find("libasync")!=-1)

os.environ("HDF5_CACHE_WR")="yes"
os.environ["HDF5_VOL_CONNECTOR"]="cache_ext config=conf1.dat;under_vol=512;under_info={under_vol=0;under_info={}}"
os.environ["HDF5_PLUGIN_PATH"]=os.environ["HDF5_PLUGIN_PATH"]+":"+os.environ["HDF5_VOL_DIR"]

def test_file():
    setup_env()
    os.system("HDF5_CACHE_WR=yes mpirun -np 2 ./test_file")
