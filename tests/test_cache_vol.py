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


def readhdf5(fst):
    env = os.environ
    print(env)
    env['HDF5_VOL_CONNECTOR']=""
    env["HDF5_PLUGIN_PATH"]=""
    cmd = ['h5dump', fst, '>&','h5.out']
    r=subprocess.run(cmd, env=env)
    
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
    def test3_file(self) -> None:
        cmd = getMPICommand(nproc=2, ppn=2)
        cmd = cmd.split()
        cmd = cmd + ['test_file.exe']
        r=subprocess.run(cmd)
        assert(r.returncode==0)
    def test3_group(self) -> None:
        cmd = getMPICommand(nproc=2, ppn=2)
        cmd = cmd.split()
        cmd = cmd + ['test_group.exe']
        r=subprocess.run(cmd)
        #readhdf5("parallel_file.h5")
        assert(r.returncode==0)
    def test3_dataset(self) -> None:
        cmd = getMPICommand(nproc=2, ppn=2)
        cmd = cmd.split()
        cmd = cmd + ['test_dataset.exe']
        r=subprocess.run(cmd)
        #readhdf5("parallel_file.h5")
        assert(r.returncode==0)

if __name__ == '__main__':
    unittest.main()
