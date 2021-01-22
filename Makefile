#Makefile
#!/bin/bash
#---------------------------------------------------------
# Make sure the following environmental variables are set:
#    HDF5_ROOT
#    HDF5_VOL_CONNECTOR
#    HDF5_PLUGIN_PATH
#----------------------------------------------------------    

HDF5_VOL_DIR=$(HDF5_ROOT)/../vol

all: vol benchmarks stack_vols

vol:
	cd src; make

benchmarks:
	cd bechmarks; make 

stack_vols:
	[ -e $(HDF5_VOL_DIR) ] || mkdir $(HDF5_VOL_DIR)
	[ -e $(HDF5_VOL_DIR)/bin ] || mkdir $(HDF5_VOL_DIR)/bin
	cp utils/stack_vols.py $(HDF5_VOL_DIR)/bin/

clean:
	cd src; make clean; cd ../benchmarks; make clean
