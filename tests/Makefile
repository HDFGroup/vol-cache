#Makefile
#!/bin/sh
include ../make.inc

CFLAGS += -I$(HDF5_VOL_DIR)/include
VOL_DIR=$(HDF5_VOL_DIR)

LIBS += ../utils/debug.o -L$(HDF5_ROOT)/lib -lhdf5 -L$(VOL_DIR)/lib  -lcache_new_h5api 
all: test_file test_group test_dataset test_dataset_async_api test_attribute

test_file: test_file.o ../utils/debug.o
	$(CXX) $(CFLAGS) -o $@ test_file.o  $(LIBS) 

test_write_multi: test_write_multi.o ../utils/debug.o
	$(CXX) $(CFLAGS) -o $@ test_write_multi.o  $(LIBS) 

test_dataset: test_dataset.o ../utils/debug.o 
	$(CXX) $(CFLAGS) -o $@ test_dataset.o  $(LIBS) 


test_attribute: test_attribute.o ../utils/debug.o 
	$(CXX) $(CFLAGS) -o $@ test_attribute.o  $(LIBS) 

test_dataset_extent: test_dataset_extent.o ../utils/debug.o 
	$(CXX) $(CFLAGS) -o $@ test_dataset_extent.o  $(LIBS) 

test_dataset_async_api: test_dataset_async_api.o ../utils/debug.o 
	$(CXX) $(CFLAGS) -o $@ test_dataset_async_api.o  $(LIBS) 

test_group: test_group.o ../utils/debug.o
	$(CXX) $(CFLAGS) -o $@ test_group.o $(LIBS) 

clean:
	rm -rf $(TARGET) *.o parallel_file.h5* test_write_cache test_read_cache *.btr prepare_dataset mpi_profile.* core test_file test_dataset test_group test_dataset_async_api

new_h5api_ex: new_h5api_ex.o
	$(CXX) $(CFLAGS) -o $@ new_h5api_ex.o $(LIBS) 

