#!/usr/bin/env python
# ---------------------------------------------------------------------------------
# This is for creating HDF5_VOL_CONNECTOR environment variables for VOL connectors;
# python stack_vols.py cache_ext async cache_ext async
# ---------------------------------------------------------------------------------

vols={'pass_through_ext':517, "cache_ext":518, "async":707}
import sys, os
nvols = len(sys.argv[1:])
print("Number of VOLs in the stack: %d" %nvols)
for v in sys.argv[1:]:
    try:
        print("* %s - %s" %(v, vols[v]))
    except:
        print("VOL %s do not exist" %v)
        exit()

s =""
i = 1
n=1
s = "%s "%sys.argv[1]
pv=sys.argv[1]
for v in sys.argv[2:]:
    if (i < nvols):
        print(pv)
        if (pv=="cache_ext"):
            s = s+ "config=conf%s.dat;under_vol=%s;under_info={"%(n,vols[sys.argv[i+1]])
            n=n+1
        else:
            s = s+ "under_vol=%s;under_info={"%(vols[sys.argv[i+1]])
    pv = sys.argv[i+1]
    i=i+1


if pv=="cache_ext":
    s = s+ "config=conf%s.dat;under_vol=0;under_info={}"%n
else:
    s = s+ "under_vol=0;under_info={}"
for i in range(nvols-1):
    s=s+"}"
print("export HDF5_PLUGIN_PATH=%s"%os.environ["HDF5_ROOT"]+"../vol/lib")
print("export HDF5_VOL_CONNECTOR=\"%s\""%s)
print("export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:$HDF5_PLUGIN_PATH")
