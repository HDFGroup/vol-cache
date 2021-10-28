#!/usr/bin/env python
from mpi4py import MPI
import h5py
import tensorflow as tf
import numpy as np
import random
comm = MPI.COMM_WORLD
nproc = comm.size
rank = comm.rank
from tqdm import tqdm 
import numpy as np
import argparse
parser = argparse.ArgumentParser(description='HDF5 Python TEST.')
parser.add_argument('--input', type=str, help="Input of the HDF5 file", default='./images.h5')
parser.add_argument('--dataset', type=str, help="Name of the dataset", default="dataset")
parser.add_argument('--num_batches', type=int, help="number of batches to read", default=32)
parser.add_argument('--shuffle', action='store_true', help="shuffle or not")
parser.add_argument("--epochs", type=int, default=4)
parser.add_argument("--batch_size", type=int, default=32)
parser.add_argument("--app_mem", type=float, default=0)
args = parser.parse_args()


import time


def getPPN():
    nodename =  MPI.Get_processor_name()
    nodelist = comm.allgather(nodename)
    nodelist = list(dict.fromkeys(nodelist))
    return comm.size // len(nodelist)

fd = h5py.File(args.input, 'r', driver='mpio', comm=comm)

class HDF5Generator:
    def __init__(self, file, xpath="data", batch_size = 16, shuffle=False, comm=None):
        self.file = file
        self.batch_size = batch_size
        self.shuffle = shuffle
        self.dset= self.file[xpath]
        self.num_samples = self.dset.shape[0]
        self.xshape = np.asarray(self.dset.shape)
        self.datatype = self.dset.dtype
        self.index_list = np.arange(self.num_samples)
        if (self.shuffle):
            np.random.shuffle(self.index_list)
        if (comm!=None):
            ns_loc = self.num_samples // comm.size
            ns_off = ns_loc*comm.rank
            self.comm = comm
        else:
            ns_loc = self.num_samples
            ns_off = 0
            class Comm:
                def __init__(self,):
                    self.rank = 0
                    self.size = 1
            self.comm = Comm()
        self.local_index_list = iter(self.index_list[ns_off:ns_off+ns_loc])
    def __iter__(self):
        return self
    def __next__(self):
        select = []
        for i in range(self.batch_size):
            a = next(self.local_index_list)
            select.append(a)
        select.sort()
        return self.dset[select]
    def reset(self):
        ns_loc = self.num_samples // self.comm.size
        ns_off = ns_loc*rank
        if (self.shuffle):
            np.random.shuffle(self.index_list)
        self.local_index_list = iter(self.index_list[ns_off:ns_off+ns_loc])

h5 = HDF5Generator(fd, args.dataset, batch_size = args.batch_size, shuffle=args.shuffle, comm=comm)

if rank==0:
    print("Number of Images: ", h5.num_samples)
    print("Images read per epoch: ", args.batch_size*args.num_batches*nproc)
    print("Datatype: ", h5.dset.dtype)
    print("Batch size: ", args.batch_size)
b = np.prod(h5.xshape[1:])
x = np.array(1, dtype=h5.dset.dtype)
ppn = getPPN()
rate=args.batch_size*b/1024/1024*nproc*x.nbytes*args.num_batches
mem = args.batch_size*b/1024/1024*x.nbytes*args.num_batches*ppn


args.app_mem = int(args.app_mem*1024)
#from guppy import hpy; h=hpy()
dd = np.zeros((args.app_mem, 1024, 1024), dtype=np.uint8)
if (args.app_mem>0):
    it = range(args.app_mem)
    if comm.rank==0:
        print("Allocating memory ...")
        it = tqdm(it)
    for i in it:
        dd[i] = i*np.ones((1024, 1024), dtype=np.uint8)
    if comm.rank==0:
        print("Done ...")
        #print(h.heap())
if comm.rank==0:
    print("========")
    print("   Number of processes per node: %d" %ppn)
    print("   Total memory: %6.3f(?) + %6.3f = %6.3f MiB"%(mem, args.app_mem*ppn, mem + args.app_mem*ppn))
cc = np.zeros((1024,1024),dtype=np.uint8)    
for e in range(args.epochs):
    t0 = time.time()            
    if (rank==0):
        it = tqdm(range(args.num_batches), desc=" Epoch %d: "%e,  total=args.num_batches, ncols=75);
    else:
        it = range(args.num_batches)
    for b in it:
        bd = next(h5)
    t1 = time.time()
    if (args.app_mem > 0):
        itm = tqdm(range(args.app_mem), desc="  APP_MEMORY")
        if (comm.rank>0):
            itm = range(args.app_mem)
        for i in itm:
            dd[i] = i*np.ones((1024, 1024), dtype=np.uint8)
    if comm.rank==0:
        print("    Bandwidth: %s MiB/sec" %(rate/(t1 - t0)))
#        print(h.heap())
    h5.reset()
fd.close()
