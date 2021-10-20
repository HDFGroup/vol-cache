#Makefile
#!/bin/sh

CC=mpicc 
CXX=mpicxx 

HDF5_DIR=$(HDF5_ROOT)
VOL_DIR=$(HDF5_VOL_DIR)
INCLUDES=-I$(HDF5_DIR)/include -I../utils/ -fPIC -I$(VOL_DIR)/include

CFLAGS=$(INCLUDES) $(DEBUG) -g -O2

CXXFLAGS=$(INCLUDES) $(DEBUG) -g -O2


%.o : %.cpp
	$(CXX) $(CXXFLAGS) -o $@ -c $<

%.o : %.c
	$(CC) $(CFLAGS) -o $@ -c $<

