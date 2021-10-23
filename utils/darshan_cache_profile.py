#!/usr/bin/env python
# This utility is for analyzing the darshan trace result
import subprocess, argparse, os
import numpy
import pandas as pd
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('darshan', type=str,
                    help='darshan profiling output')
parser.add_argument('-o', '--output', type=str, default="write-cache.json", 
                    help='write cache profiling')

args = parser.parse_args()
def RUNCMD(cmd):
    return subprocess.run(cmd.split(" "), stdout=subprocess.PIPE)

def loadDarshanLog(fin):
    log={}
    result = RUNCMD("darshan-dxt-parser %s >& /tmp/darshan.log" %args.darshan)
    lines = result.stdout.decode('utf-8').split("\n")
    io_lines = False
    pb_total = len(lines)
    df = pd.DataFrame(index=numpy.arange(pb_total),
                      columns=['Module', 'Filename', 'Rank', 'Operation', 'Segment', 'Offset', 'Length', 'Start',
                               'End'])
    temp_filename = ""
    i = 1
    index = 0
    h5 = ""
    for line in lines:
        if i % 100 == 0 or i == pb_total:
            progress(i, pb_total, status='Parsing DXT File')
            i += 1
        if line == '':
            io_lines = False
            continue
        elif "DXT, file_id" in line:
            temp_filename = line.split(" ")[5]
            if ("-cache/mmap-" in temp_filename):
                h5 = temp_filename.split("/")[-2].split("-cache")[0]
            elif (temp_filename[-2:]=="h5"):
                h5 = temp_filename.split("/")[-1]
            io_lines = False
            continue
        elif "Module" in line:
            io_lines = True
        elif io_lines:
            # Module,Rank, Wt/Rd, Segment,Offset,Length,Start(s),End(s)
            vals = line.split()
            df.loc[index] = {'Module': vals[0],
                             'Filename': temp_filename.split("/")[-1],
                             'Rank': int(vals[1]),
                             'Operation': vals[2],
                             'Segment': int(vals[3]),
                             'Offset': int(vals[4]),
                             'Length': int(vals[5]),
                             'Start': float(vals[6]),
                             'End': float(vals[7])}
            index += 1
        elif "# " in line and ": " in line:
            s= line.split(": ")
            log[s[0][2:]] = s[1]
    df = df.drop(df.index[index:])
    log["io"] = df
    log["h5"] = h5
    return log
log = loadDarshanLog(args.darshan)
df = log["io"]
#for (int i=0; i<int(log['nprocs']); i++):

def createEvent(p, rank=0, thread=0, name="mmap-0.dat", cat='w'):
    eb ={
        "name": name,
        "cat": cat,
        "ts": p[0],
        "pid": rank,
        "tid": thread,
        "ph": "B",
    }
    ee =  {
        "name": name,        
        "ts": p[1],
        "cat": cat, 
        "pid": rank,
        "tid": thread,
        "ph": "E", 
    }
    return eb, ee
p = []
for i in range(int(log['nprocs'])):
    cache = df[(df.Rank==i) & (df.Filename=="mmap-%s.dat"%i)][["Start", "End"]].to_numpy()
    cache = cache*1000000 + float(log["start_time"])*1000000
    pfs_posix = df[(df.Rank==i) & (df.Filename==log["h5"])][["Start", "End"]].to_numpy()
    pfs_mpiio = df[(df.Rank==i) & (df.Filename==log["h5"]) & (df.Module=="X_MPIIO")][["Start", "End"]].to_numpy()
    pfs_posix = df[(df.Rank==i) & (df.Filename==log["h5"]) & (df.Module=="X_POSIX")][["Start", "End"]].to_numpy()
    pfs_mpiio = pfs_mpiio*1000000 + float(log["start_time"])*1000000
    pfs_posix = pfs_posix*1000000 + float(log["start_time"])*1000000
    for f in cache:
        a, b = createEvent(f, rank=i, name="mmap-%s.dat"%i, thread=0, cat="X_POSIX")
        p.append(a)
        p.append(b)
    for f in pfs_mpiio:
        a, b = createEvent(f, rank=i, name=log["h5"], thread=1, cat="X_POSIX")
        p.append(a)
        p.append(b)
    for f in pfs_posix:
        a, b = createEvent(f, rank=i, name=log["h5"], thread=1, cat="X_MPIO")
        p.append(a)
        p.append(b)
timeline = {"traceEvents":p}
import json
f = open(args.output, 'w')
f.write(json.dumps(timeline, indent=4))
f.close()
