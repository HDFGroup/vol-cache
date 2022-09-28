#!/usr/bin/env python
# This is still under development...
import os, sys
import subprocess

def setup_env():
    os.system("stack_vols.py cache_ext async")
    os.environ['HDF5_PLUGIN_PATH']=os.environ['HDF5_VOL_DIR']
    os.environ['HDF5_VOL_CONNECTOR'] = "cache_ext config=conf1.dat;under_vol=512;under_info={under_vol=0;under_info={}}"
    os.environ["LD_LIBRARY_PATH"] = os.environ["LD_LIBRARY_PATH"] + ":"+os.environ['HDF5_PLUGIN_PATH']
    os.environ["HDF5_CACHE_WR"] = "yes"
def test_env():
    print(os.environ["HDF5_ROOT"], os.environ['HDF5_VOL_DIR'])
    
def test_file():
    setup_env()
    subprocess.run(["mpirun -np 2 ./test_file"], shell=True, check=True)
    subprocess.run(["mpirun -np 2 ./test_file"], shell=True, check=True)

def test_group():    
    setup_env()
    os.system("HDF5_CACHE_WR=yes mpirun -np 2 ./test_group")
    os.system("HDF5_CACHE_WR=no mpirun -np 2 ./test_group")

def test_dataset():    
    setup_env()
    os.system("HDF5_CACHE_WR=yes mpirun -np 2 ./test_dataset")
    os.system("HDF5_CACHE_WR=no mpirun -np 2 ./test_dataset")

