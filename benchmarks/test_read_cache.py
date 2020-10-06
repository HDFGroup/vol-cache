#!/usr/bin/env python
from mpi4py import MPI
import numpy as np
comm = MPI.COMM_WORLD
nproc = comm.size
rank = comm.rank
from tqdm import tqdm 
import h5py

import argparse
parser = argparse.ArgumentParser(description='HDF5 Python TEST.')
parser.add_argument('--input', type=str, help="Input of the HDF5 file", default='./images.h5')
parser.add_argument('--dataset', type=str, help="Name of the dataset", default="dataset")
parser.add_argument('--num_batches', type=int, help="number of batches to read", default=32)
parser.add_argument('--shuffle', action='store_true', help="shuffle or not")
parser.add_argument("--epochs", type=int, default=8)
parser.add_argument("--batch_size", type=int, default=32)
args = parser.parse_args()

f = h5py.File(args.input, 'r', driver='mpio', comm=comm)
dset=f[args.dataset]
nimages=dset.shape[0]
if rank==0:
    print("Number of Images: ", nimages)
    print("images read per epoch: ", args.batch_size*args.num_batches*nproc)
lst = np.arange(nimages)
ns_loc = nimages//nproc
ns_off = ns_loc*rank
import time
for e in range(args.epochs):
    t0 = time.time()
    if args.shuffle:
        np.random.shuffle(lst)
    if (rank==0):
        it = tqdm(range(args.num_batches));
    else:
        it = range(args.num_batches)
    for b in it:
        select = lst[ns_off+b*args.batch_size:ns_off+(b+1)*args.batch_size]
        select.sort()
        bd = dset[select]
    t1 = time.time()
    if (rank==0):
        print("Epoch %d: %5.2f MB/s"%(e, args.num_batches*args.batch_size*224*224*3*4/(t1-t0)/1024/1024*nproc))
f.close()
