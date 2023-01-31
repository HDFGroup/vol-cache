#!/usr/bin/env python
import unittest
import os
import sys
import glob
import platform
import yaml
import subprocess
from subprocess import Popen, PIPE
import socket
host=platform.system()

if host=="Darwin":
    libend="dylib"
else:
    libend = "so"
MPI = ['jsrun', 'aprun', 'mpirun']

def getTiming():
    fin = open("write_cache.out", 'r')
    l = fin.readline()        
    while(l.find("Timing Information")==-1):
        l = fin.readline()
    tt = {}
    while(l.find("compute")==-1):
        l=fin.readline()
    tt['compute'] = float(l.split()[2])
    while(l.find("H5Fcache_wait")==-1):
        l=fin.readline()
    tt['H5Fcache_wait'] = float(l.split()[2])
    while(l.find("H5Dwrite")==-1):
        l=fin.readline()
    tt['H5Dwrite'] = float(l.split()[2])
    while(l.find("close")==-1):
        l = fin.readline()
    tt['close'] = float(l.split()[2])
    fin.close()
    return tt

def getMPICommand(nproc=1, ppn=None):
    for mpi in MPI:
        result = subprocess.run(["which", mpi], capture_output=True)
        if result.returncode==0:
            if result.stdout !=None:
                cmd = str(str(result.stdout).split("/")[-1])[:-3]
    hostname = socket.gethostname()
    if hostname=="zion":
        return f"aprun -n {nproc}"
    if ppn == None:
        ppn = nproc
    if (cmd=="aprun"):
        return f"aprun -n {nproc} -N {ppn}"
    if (cmd=="mpirun"):
        return f"mpirun -np {nproc}"


def readhdf5(fstr):
    env = os.environ.copy()
    del env['HDF5_VOL_CONNECTOR']
    del env['HDF5_PLUGIN_PATH']
    output = open('h5.out', 'w')
    cmd = f"h5dump {fstr}"
    r=subprocess.run(cmd.split(), env=env, stdout=output)
    output.close()

class TestCacheVOL(unittest.TestCase):
    def readConfig(self, fcfg):
        with open(fcfg, 'r') as file:
            cfg = yaml.safe_load(file)
        return cfg
    def test_0_connector_setup(self) -> None:
        assert("HDF5_VOL_CONNECTOR" in os.environ)
        self.connector=os.environ["HDF5_VOL_CONNECTOR"]
    def test_0_path_setup(self) -> None:
        assert("HDF5_PLUGIN_PATH" in os.environ)
        self.plugin_path = os.environ["HDF5_PLUGIN_PATH"]
        assert(os.path.exists(self.plugin_path))
        libs=[os.path.basename(a) for a in glob.glob("%s/lib*.*"%self.plugin_path)]
        assert("libh5cache_vol.%s"%(libend) in libs)
        assert("libcache_new_h5api.a" in libs)
        assert("libasynchdf5.a" in libs)
        assert("libh5async.%s"%(libend) in libs)
    def test_0_config_setup(self)->None:
        assert("HDF5_VOL_CONNECTOR" in os.environ)
        self.vol_connector = os.environ["HDF5_VOL_CONNECTOR"]
        print(self.vol_connector)
        if self.vol_connector.find("cache_ext")==-1:
            raise Exception("Cache VOL is not specified in HDF5_VOL_CONNECTOR")
        else:
            config = self.vol_connector.split("config=")[1].split(";")[0]
            print(f"config file is: {config}")
            cfg = self.readConfig(config)
            for k in cfg.keys():
                print(f" {k}: {cfg[k]}")
            if cfg["HDF5_CACHE_STORAGE_TYPE"]!="MEMORY":
                self.storage_path = cfg["HDF5_CACHE_STORAGE_PATH"]
                if (not os.path.exists(self.storage_path)):
                    raise Exception(f"STORAGE PATH: {self.storage_path} does not exist")
    def test_3_file(self) -> None:
        cmd = getMPICommand(nproc=2, ppn=2)
        cmd = cmd.split()
        cmd = cmd + ['test_file.exe']
        r=subprocess.run(cmd)
        assert(r.returncode==0)
    def test_3_group(self) -> None:
        cmd = getMPICommand(nproc=2, ppn=2)
        cmd = cmd.split()
        cmd = cmd + ['test_group.exe']
        r=subprocess.run(cmd)
        #readhdf5("parallel_file.h5")
        assert(r.returncode==0)
    def test_3_dataset(self) -> None:
        cmd = getMPICommand(nproc=2, ppn=2)
        cmd = cmd.split()
        cmd = cmd + ['test_dataset.exe']
        r=subprocess.run(cmd)
        readhdf5("parallel_file.h5")
        f = open("h5.out", 'r')
        f.readline()
        f.readline()
        a=f.readline().split()
        assert(a[0]=="GROUP")
        assert(a[1]=="\"0\"")
        a, b, c = f.readline().split()
        assert(a=="DATASET")
        assert(b=="\"dset_test\"")
        f.readline()
        f.readline()
        f.readline()
        a, b = f.readline().split(":")
        a = a.strip()
        b = b.strip()

        assert(a=="(0,0)" and b=="1, 1,")
        a, b = f.readline().split(":")
        a = a.strip()
        b = b.strip()
        assert(a=="(1,0)" and b=="1, 1,")
        a, b = f.readline().split(":")
        a = a.strip()
        b = b.strip()
        assert(a=="(2,0)" and b.strip()=="2, 2,")
        a, b = f.readline().split(":")
        a = a.strip()
        b = b.strip()
        print(a, b)
        assert(a=="(3,0)" and b.strip()=="2, 2")
        assert(r.returncode==0)
        f.close()
    def test_cache_write(self) -> None:
        cmd = getMPICommand(nproc=2, ppn=2)
        cmd = cmd.split()
        cmd = cmd + ['write_cache.exe'] + ["--sleep"] + ["1.0"] + ["--niter"] + ['8']
        fout = open("write_cache.out", 'w')
        r=subprocess.run(cmd, stdout=fout)
        fout.close()
        tt = getTiming()
        os.environ["HDF5_CACHE_WR"]="yes"
        fout = open("write_cache.out", 'w')
        r=subprocess.run(cmd, stdout=fout)
        fout.close()
        tt2 = getTiming()
        print(tt)
        print(tt2)

if __name__ == '__main__':
    unittest.main()
