#Makefile
#!/bin/bash
#---------------------------------------------------------
# Make sure the following environmental variables are set:
#    HDF5_ROOT
#    HDF5_VOL_CONNECTOR
#    HDF5_PLUGIN_PATH
#----------------------------------------------------------    
all: vol benchmarks

vol:
	cd src; make
benchmarks:
	cd bechmarks; make 
clean:
	cd src; make clean; cd ../benchmarks; make clean
