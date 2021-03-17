#Makefile
#!/bin/sh
include ../make.inc

HDF5_DIR=$(HDF5_ROOT)
VOL_DIR=$(HDF5_DIR)/../vol
INCLUDES +=-I$(HDF5_DIR)/include -I../utils/ -fPIC 
INCLUDES += -I/usr/local/cuda/include
LIBS +=-L$(HDF5_DIR)/lib -lhdf5
LIBS +=-Wl,-rpath=$(HDF5_DIR)/lib -L /usr/local/cuda/lib64 -lcuda -lcudart
#DEBUG +=-DENABLE_EXT_CACHE_LOGGING 

CFLAGS +=$(INCLUDES) $(DEBUG) -g #-current_version 1.0 -dynamiclib


UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
        LIBNAME=so
else
        LIBNAME=dylib
endif

TARGET=libh5cache_vol.${LIBNAME}

all: makeso cache_new_h5api
makeso: H5VLcache_ext.o cache_utils.o ../utils/debug.o H5LS.o H5LS_SSD.o H5LS_RAM.o H5LS_GPU.o
	$(CC) -fPIC -shared $(CFLAGS)  $(DEBUG) -o $(TARGET) -fPIC H5LS.o H5VLcache_ext.o cache_utils.o H5LS_SSD.o H5LS_RAM.o H5LS_GPU.o ../utils/debug.o $(LIBS)
	[ -e $(VOL_DIR) ] || mkdir $(VOL_DIR)
	[ -e $(VOL_DIR)/lib ] || mkdir $(VOL_DIR)/lib
	[ -e $(VOL_DIR)/include ] || mkdir $(VOL_DIR)/include
	mv $(TARGET) $(VOL_DIR)/lib
	cp H5VLcache_ext.h cache_utils.h H5LS.h cache_new_h5api.h $(VOL_DIR)/include


cache_new_h5api: cache_new_h5api.o
	ar -cr libcache_new_h5api.a cache_new_h5api.o
	mv libcache_new_h5api.a $(VOL_DIR)/lib
	cp cache_new_h5api.h $(VOL_DIR)/include

h5vlp: H5VLP.o
	$(CC) -o h5vlp.x H5VLP.o $(LIBS)
clean:
	rm -f $(TARGET) *.o parallel_file.h5* test_write_cache test_read_cache
