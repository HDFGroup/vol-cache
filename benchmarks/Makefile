#Makefile
#!/bin/sh
include ../make.inc

all: test_write_cache test_read_cache prepare_dataset 

CFLAGS += -I$(HDF5_VOL_DIR)/include 

LIBS += ../utils/debug.o -L$(HDF5_ROOT)/lib -lhdf5 -L$(VOL_DIR)/lib -lcache_new_h5api
all: prepare_dataset test_write_cache test_read_cache test_write

test_write_cache: test_write_cache.o ../utils/debug.o profiling.o
	$(CXX) $(CXXFLAGS) -o $@ test_write_cache.o profiling.o $(LIBS) 

test_write: test_write.o ../utils/debug.o profiling.o
	$(CXX) $(CXXFLAGS) -o $@ test_write.o profiling.o $(LIBS) 

prepare_dataset: prepare_dataset.o ../utils/debug.o 
	$(CXX) $(CXXFLAGS) -o $@ prepare_dataset.o ../utils/debug.o -L$(HDF5_ROOT)/lib -lhdf5 

test_read_cache: test_read_cache.o ../utils/debug.o profiling.o
	$(CXX) $(CXXFLAGS) -o $@ test_read_cache.o profiling.o $(LIBS) ../src/cache_utils.o

clean:
	rm -rf $(TARGET) *.o parallel_file.h5* test_write_cache test_read_cache *.btr prepare_dataset mpi_profile.* core

new_h5api_ex: new_h5api_ex.o
	$(CXX) $(CXXFLAGS) -o $@ new_h5api_ex.o $(LIBS) 

