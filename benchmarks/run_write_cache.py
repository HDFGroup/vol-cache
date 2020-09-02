#!/usr/bin/env python
import os
import argparse

os.environ['ROMIO_PRINT_HINTS']='1'
os.environ['OMPI_LD_PRELOAD_PREPEND']='/ccs/home/walkup/mpitrace/spectrum_mpi/libmpitrace.so'
parser = argparse.ArgumentParser(description='Process some integers.')
def recMkdir(string):
    dd = os.environ['PWD']
    directories = string.split('/')
    for d in directories:
        os.system("[ -e %s ] || mkdir %s" %(d, d))
        os.chdir(d)
    os.chdir(dd)

parser.add_argument("--num_nodes", type=int, default=1)
parser.add_argument("--ppn", type=int, default=16)
parser.add_argument("--ind", action='store_true')
parser.add_argument("--ccio", action='store_true')
parser.add_argument("--ntrials", type=int, default=16)
parser.add_argument("--nvars", type=int, default=8)
parser.add_argument("--cb_nodes", type=int, default=1)
parser.add_argument("--cb_size", type=str, default='16m')
parser.add_argument("--fs_block_size", type=str, default='16m')
parser.add_argument("--num_particles", type=int, default=8)
parser.add_argument("--fs_block_count", type=int, default=1)
parser.add_argument("--romio_cb_nodes", type=int, default=None)
parser.add_argument("--romio_cb_size", type=str, default=None)
parser.add_argument("--stdout", action='store_true')
parser.add_argument("--align", type=str, default=None)
parser.add_argument("--directory", type=str, default=None)
parser.add_argument("--sleep", type=float, default=0.1)
args = parser.parse_args()
def bytes(string):
    if string.find('m')!=-1:
        return int(string[:-1])*1024*1024
    elif string.find('k')!=-1:
        return int(string[:-1])*1024
    elif string.find('g')!=-1:
        return int(string[:-1])*1024*1024*1024
    else:
        return int(string)
cb_size = bytes(args.cb_size)
if (bytes(args.cb_size)<bytes(args.fs_block_size)):
    args.fs_block_size = args.cb_size
fs_block_size =bytes(args.fs_block_size)
if (args.align !=None):
    os.environ['ALIGNMENT']=str(bytes(args.align))

if (args.ind):
    output="ind.n%s.p%s.np%s" %(args.num_nodes, args.ppn, args.num_particles)
else:
    output="n%s.p%s.np%s" %(args.num_nodes, args.ppn, args.num_particles)
if ((args.romio_cb_nodes!=None) or (args.romio_cb_size != None)):
    fin = open("romio_hints", 'w')
    fin.write("romio_cb_write enable\n")
    if (args.romio_cb_nodes!=None):
        fin.write("cb_nodes %s\n"%args.romio_cb_nodes)
        sp = ((args.num_nodes*args.ppn)) // args.romio_cb_nodes
        s = " "
        for i in range(args.romio_cb_nodes):
            s = s+ " %s " %(i*sp)
        fin.write("cb_aggregator_list %s\n"%s)
        fin.write("cb_config_list *:%s\n"%(args.romio_cb_nodes//args.num_nodes))
    else:
        args.romio_cb_nodes=args.num_nodes
    if (args.romio_cb_size!=None):
        fin.write("cb_buffer_size %s\n"%bytes(args.romio_cb_size))
    else:
        args.romio_cb_size='16m'
    fin.close()
    os.environ['ROMIO_HINTS']=os.environ['PWD']+"/romio_hints"
    output = output + ".rcn%s.rcs%s"%(args.romio_cb_nodes, args.romio_cb_size)

if args.ccio:
    output=output+".cn%s.cs%s.fb%s.ccio"%(args.cb_nodes, args.cb_size, args.fs_block_size)

execname = "/ccs/home/hzheng/ExaHDF5/node_local_storage/cache_vol/benchmarks/test_write_cache"

if args.ccio:
    os.environ["HDF5_CCIO_ASYNC"]="yes"
    os.environ['HDF5_CCIO_FS_BLOCK_SIZE']=str(fs_block_size)
    os.environ['HDF5_CCIO_FS_BLOCK_COUNT']=str(args.fs_block_count)
    os.environ["HDF5_CCIO_WR"]="yes"
    os.environ["HDF5_CCIO_RD"]="yes"
    os.environ["HDF5_CCIO_DEBUG"]="yes"
    os.environ["HDF5_CCIO_CB_NODES"]=str(args.cb_nodes)
    os.environ["HDF5_CCIO_CB_SIZE"]=str(cb_size)
if (args.ind):
    collective = 0
else:
    collective = 1
if (args.ppn%2==0):
    cmd = "jsrun --smpiarg=\"--async\" -n %s -c 21 -a %s %s --collective %s --niter %s --sleep %s --nvars %s" %(args.num_nodes*2, args.ppn//2, execname, collective, args.ntrials, args.sleep, args.nvars)
else:
    cmd = "jsrun --smpiarg=\"--async\" -n %s -c 42 -a %s %s --collective %s --niter %s --sleep %s --nvars %s" %(args.num_nodes, args.ppn, execname, collective, args.ntrials, args.sleep, args.nvars)

print(cmd)


if args.directory!=None:
    output=args.directory

recMkdir(output)
dd = os.environ['PWD']
os.chdir(output)
if (args.stdout):
    os.system(cmd)
else:
    os.system(cmd+" >> results")
#os.system("rm -r parallel_file.h5")
os.chdir(dd)
