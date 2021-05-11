#!/usr/bin/env python
import h5py
from mpi4py import MPI
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
parser.add_argument("--epochs", type=int, default=8)
parser.add_argument("--batch_size", type=int, default=32)
args = parser.parse_args()

#f = h5py.File(args.input, 'r', driver='mpio', comm=comm)
#dset=f[args.dataset]
import time

#nimages=dset.shape[0]

fd = h5py.File(args.input, 'r', driver='mpio', comm=comm)
class HDF5Generator:
    def __init__(self, file, batch_size = 16, shuffle=False):
        self.file = file
        self.batch_size = batch_size
        self.shuffle = shuffle
        self.dset= self.file[args.dataset]
        self.num_samples = self.dset.shape[0]
        self.index_list = np.arange(self.num_samples)
        if (self.shuffle):
            np.random.shuffle(self.index_list)
        ns_loc = self.num_samples // nproc
        ns_off = ns_loc*rank
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
        ns_loc = self.num_samples // nproc
        ns_off = ns_loc*rank
        if (self.shuffle):
            np.random.shuffle(self.index_list)
        self.local_index_list = iter(self.index_list[ns_off:ns_off+ns_loc])

h5 = HDF5Generator(fd, batch_size = args.batch_size, shuffle=args.shuffle)

if rank==0:
    print("Number of Images: ", h5.num_samples)
    print("images read per epoch: ", args.batch_size*args.num_batches*nproc)

rate=args.batch_size*224*224*3*4/1024/1024*nproc


for e in range(args.epochs):
    t0 = time.time()
    if (rank==0):
        it = tqdm(range(args.num_batches), desc=" Epoch %d: "%e, unit=" MB", unit_scale=rate, total=args.num_batches, ncols=100);
    else:
        it = range(args.num_batches)
    for b in it:
        bd = next(h5)
    t1 = time.time()
    h5.reset()
fd.close()
