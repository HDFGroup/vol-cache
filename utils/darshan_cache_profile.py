#!/usr/bin/env python
# This utility is for analyzing the darshan trace result
import subprocess, argparse, os
import numpy as np
import pandas as pd
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('darshan', type=str,
                    help='darshan profiling output')
parser.add_argument('-o', '--output', type=str, default="write-cache.json", 
                    help='write cache profiling')

args = parser.parse_args()
def RUNCMD(cmd):
    return subprocess.run(cmd.split(" "), stdout=subprocess.PIPE)

def createEvent(p, rank=0, thread=0, name="mmap-0.dat", cat='w', args={}):
    eb ={
        "name": name,
        "cat": cat,
        "ts": p[0],
        "pid": rank,
        "tid": thread,
        "ph": "B",
        "args": args
    }
    ee =  {
        "name": name,        
        "ts": p[1],
        "cat": cat, 
        "pid": rank,
        "tid": thread,
        "ph": "E",
        "args": args
    }
    return [eb, ee]

def loadDarshanLog(fin):
    log={}
    result = RUNCMD("darshan-dxt-parser %s" %fin)
    f = open("/tmp/darshan.log", 'w')
    f.write(result.stdout.decode('utf-8'))
    f.close()
    lines = result.stdout.decode('utf-8').split("\n")
    io_lines = False
    pb_total = len(lines)
    df = pd.DataFrame(index=np.arange(pb_total),
                      columns=['Module', 'Filename', 'Rank', 'Operation', 'Segment', 'Offset', 'Length', 'Start',
                               'End'])
    temp_filename = ""
    i = 1
    index = 0
    h5 = ""
    fcache=""
    p = []
    for line in lines:
        if i % 100 == 0 or i == pb_total:
        #    progress(i, pb_total, status='Parsing DXT File')
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
            fargs = {
                    'Operation': vals[2],
                    'Segment': int(vals[3]),
                    'Offset': int(vals[4]),
                    'Length': int(vals[5])
            }
            if "mmap" in temp_filename.split("/")[-1]:
                tid = 0
            else:
                tid = 1
            t = np.array([float(vals[6]), float(vals[7])])*1e6 + float(log["start_time"])*1e6
            p = p+createEvent(t,
                              rank=int(vals[1]), thread=tid,
                              name=temp_filename.split("/")[-1],
                              cat=vals[0], args=fargs)
        elif "# " in line and ": " in line:
            s= line.split(": ")
            log[s[0][2:]] = s[1]
    df = df.drop(df.index[index:])
    log["io"] = df
    log["h5"] = h5
    for i in range(int(log["nprocs"])):
        p = p+createEvent([float(log["start_time"])*1e6, float(log["end_time"])*1e6],
                          rank=i, thread=0,
                          name="Application",
                          cat="N/A")
    log["timeline"] = {"traceEvents":p}
    return log
if __name__ == '__main__':
    if args.darshan.find("/")==-1:
        log = loadDarshanLog(os.environ["DARSHAN_LOG_DIR"] + "/"+ args.darshan)        
    else:
        log = loadDarshanLog(args.darshan)
    import json
    f = open(args.output, 'w')
    f.write(json.dumps(log['timeline'], indent=4))
    f.close()
