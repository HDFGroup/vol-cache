#Makefile
#!/bin/bash
#---------------------------------------------------------
# Make sure the following environmental variables are set:
#    HDF5_ROOT
#    HDF5_VOL_CONNECTOR
#    HDF5_PLUGIN_PATH
#----------------------------------------------------------    

#HDF5_VOL_DIR=$(HDF5_ROOT)/../vol

all: vol microbenchmarks stack_vols
#all: vol

vol:
	cd src && make

microbenchmarks: 
	cd benchmarks && make

stack_vols:
	[ -e $(HDF5_VOL_DIR) ] || mkdir $(HDF5_VOL_DIR)
	[ -e $(HDF5_VOL_DIR)/bin ] || mkdir $(HDF5_VOL_DIR)/bin
	cp -v utils/stack_vols.py $(HDF5_VOL_DIR)/bin/
	cp -v utils/darshan_profile_timeline.py $(HDF5_VOL_DIR)/bin/

clean:
	cd src; make clean; cd ../benchmarks; make clean
