#Makefile
#!/bin/sh
include ../make.inc

CFLAGS += -I$(HDF5_VOL_DIR)/include
VOL_DIR=$(HDF5_VOL_DIR)

LIBS += ../utils/debug.o -L$(HDF5_ROOT)/lib -lhdf5 -L$(VOL_DIR)/lib  -lcache_new_h5api 
all: test_file test_group test_dataset test_dataset_async

test_file: test_file.o ../utils/debug.o
	$(CXX) $(CFLAGS) -o $@ test_file.o  $(LIBS) 

test_dataset: test_dataset.o ../utils/debug.o 
	$(CXX) $(CFLAGS) -o $@ test_dataset.o  $(LIBS) 

test_dataset_async: test_dataset_async.o ../utils/debug.o 
	$(CXX) $(CFLAGS) -o $@ test_dataset.o  $(LIBS) 

test_group: test_group.o ../utils/debug.o
	$(CXX) $(CFLAGS) -o $@ test_group.o $(LIBS) 

clean:
	rm -rf $(TARGET) *.o parallel_file.h5* test_write_cache test_read_cache *.btr prepare_dataset mpi_profile.* core

new_h5api_ex: new_h5api_ex.o
	$(CXX) $(CFLAGS) -o $@ new_h5api_ex.o $(LIBS) 

