#Makefile
#!/bin/sh

CC=mpicc -O0 
CXX=mpicxx -O0

HDF5_DIR=$(HDF5_ROOT)
VOL_DIR=$(HDF5_DIR)/../vol
INCLUDES=-I$(HDF5_DIR)/include -I../utils/ -fPIC -I$(VOL_DIR)/include
LIBS=-L$(HDF5_DIR)/lib -lhdf5 
#DEBUG=-DENABLE_EXT_CACHE_LOGGING 

CFLAGS=$(INCLUDES) $(DEBUG) -g  

%.o : %.cpp
	$(CXX) $(CFLAGS) -o $@ -c $<
%.o : %.c
	$(CC) $(CFLAGS) -o $@ -c $<

