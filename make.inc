#Makefile
#!/bin/sh

CC=mpicc 
CXX=mpicxx 

HDF5_DIR=$(HDF5_ROOT)
VOL_DIR=$(HDF5_VOL_DIR)
INCLUDES=-I$(HDF5_DIR)/include -I../utils/ -fPIC -I$(VOL_DIR)/include

CFLAGS=$(INCLUDES) $(DEBUG) -g  -std=c11

CXXFLAGS=$(INCLUDES) $(DEBUG) -g  -std=c++11


%.o : %.cpp
	$(CXX) $(CXXFLAGS) -o $@ -c $<

%.o : %.c
	$(CC) $(CFLAGS) -o $@ -c $<

