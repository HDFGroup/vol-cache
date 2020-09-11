#Makefile
#!/bin/sh
include ../make.inc

all: test_write_cache test_read_cache
CFLAGS += -I$(VOL_DIR)/include
LIBS += -L$(VOL_DIR)/lib -lh5passthrough_vol ../utils/debug.o -lpthread
all: prepare_dataset test_write_cache test_read_cache

test_write_cache: test_write_cache.o ../utils/debug.o 
	$(CXX) $(CFLAGS) -o $@ test_write_cache.o  $(LIBS) 

prepare_dataset: prepare_dataset.o ../utils/debug.o 
	$(CXX) $(CFLAGS) -o $@ prepare_dataset.o  $(LIBS) 

test_read_cache: test_read_cache.o ../utils/debug.o profiling.o
	$(CXX) $(CFLAGS) -o $@ test_read_cache.o profiling.o $(LIBS) 

clean:
	rm -f $(TARGET) *.o parallel_file.h5* test_write_cache test_read_cache *.btr prepare_dataset mpi_profile.*

new_h5api_ex: new_h5api_ex.o
	$(CXX) $(CFLAGS) -o $@ new_h5api_ex.o $(LIBS) 

