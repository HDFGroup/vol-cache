export HDF5_PLUGIN_PATH=/home/huihuo.zheng/workspace/exahdf5/soft//hdf5/vol/lib
export HDF5_VOL_CONNECTOR="cache_ext config=cache_1.cfg;under_vol=512;under_info={under_vol=0;under_info={}}"
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HDF5_PLUGIN_PATH
mkdir -p SSD
echo "
HDF5_CACHE_STORAGE_TYPE: SSD
HDF5_CACHE_STORAGE_PATH: SSD
HDF5_CACHE_STORAGE_SCOPE: LOCAL
HDF5_CACHE_STORAGE_SIZE: 1287558138880
HDF5_CACHE_WRITE_BUFFER_SIZE: 10485760000
" > cache_1.cfg
HDF5_CACHE_WR=yes mpirun -np 1 write_cache.exe --async_close --sleep 0.25 --niter 2 --nvars 1 


