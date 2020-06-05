#Makefile
#!/bin/sh

CC=mpicc
CXX=mpicxx

HDF5_DIR=$(HDF5_ROOT)

INCLUDES=-I$(HDF5_DIR)/include -I./utils/ -fPIC 
LIBS=-L$(HDF5_DIR)/lib -lhdf5 -lz
#DEBUG=-DENABLE_EXT_PASSTHRU_LOGGING 

CFLAGS=$(INCLUDES) $(DEBUG) -g

TARGET=libh5passthrough_vol.dylib

all: makeso test_write_cache

%.o : %.cpp
	$(CXX) $(CFLAGS) -o $@ -c $<
%.o : %.c
	$(CC) $(CFLAGS) -o $@ -c $<

all: test_write_cache test_read_cache

test_write_cache: test_write_cache.o ./utils/debug.o H5Dio_cache.o
	$(CXX) $(CFLAGS) -o $@ test_write_cache.o ./utils/debug.o H5Dio_cache.o $(LIBS) -L${HDF5_DIR}/../vol/ #-lh5passthrough_vol

test_read_cache: test_read_cache.o ./utils/debug.o H5Dio_cache.o
	$(CXX) $(CFLAGS) -o $@ test_read_cache.o ./utils/debug.o H5Dio_cache.o $(LIBS) -L${HDF5_DIR}/../vol/ #-lh5passthrough_vol

makeso: H5VLpassthru_ext.o H5Dio_cache.o ./utils/debug.o
	$(CC) -shared $(CFLAGS)  $(DEBUG) -o $(TARGET) -fPIC H5VLpassthru_ext.o H5Dio_cache.o ./utils/debug.o $(LIBS)
	mv  $(TARGET) $(HDF5_DIR)/../vol
clean:
	rm -f $(TARGET) *.o parallel_file.h5* test_write_cache test_read_cache


