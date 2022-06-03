#Makefile
#!/bin/sh
include ../make.inc

all: write_cache read_cache prepare_dataset 

CFLAGS += -I$(HDF5_VOL_DIR)/include 

LIBS += ../utils/debug.o -L$(HDF5_ROOT)/lib -lhdf5 -L$(VOL_DIR)/lib -lcache_new_h5api
all: prepare_dataset write_cache read_cache write

write_cache: write_cache.o ../utils/debug.o profiling.o ../src/cache_utils.o
	$(CXX) $(CXXFLAGS) -o $@ write_cache.o profiling.o ../src/cache_utils.o $(LIBS) 

write: write.o ../utils/debug.o profiling.o
	$(CXX) $(CXXFLAGS) -o $@ write.o profiling.o $(LIBS) 

prepare_dataset: prepare_dataset.o ../utils/debug.o 
	$(CXX) $(CXXFLAGS) -o $@ prepare_dataset.o ../utils/debug.o -L$(HDF5_ROOT)/lib -lhdf5 

read_cache: read_cache.o ../utils/debug.o profiling.o
	$(CXX) $(CXXFLAGS) -o $@ read_cache.o profiling.o $(LIBS) ../src/cache_utils.o

clean:
	rm -rf $(TARGET) *.o parallel_file.h5* write_cache read_cache *.btr prepare_dataset mpi_profile.* core

new_h5api_ex: new_h5api_ex.o
	$(CXX) $(CXXFLAGS) -o $@ new_h5api_ex.o $(LIBS) 

