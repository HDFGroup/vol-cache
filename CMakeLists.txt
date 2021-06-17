cmake_minimum_required(VERSION 3.12)
project(HDF5_VOL_CACHE VERSION 0.0.1 LANGUAGES C)

if(NOT HDF5_VOL_CACHE_INSTALL_BIN_DIR)
  set(HDF5_VOL_CACHE_INSTALL_BIN_DIR ${CMAKE_INSTALL_PREFIX}/bin)
endif()
if(NOT HDF5_VOL_CACHE_INSTALL_LIB_DIR)
  set(HDF5_VOL_CACHE_INSTALL_LIB_DIR ${CMAKE_INSTALL_PREFIX}/lib)
endif()
if(NOT HDF5_VOL_CACHE_INSTALL_INCLUDE_DIR)
  set(HDF5_VOL_CACHE_INSTALL_INCLUDE_DIR ${CMAKE_INSTALL_PREFIX}/include)
endif()
if(NOT HDF5_VOL_CACHE_INSTALL_DATA_DIR)
  set(HDF5_VOL_CACHE_INSTALL_DATA_DIR ${CMAKE_INSTALL_PREFIX}/share)
endif()


set(HDF5_VOL_CACHE_PACKAGE "hdf5_vol_cache")
set(HDF5_VOL_CACHE_PACKAGE_NAME "HDF5_VOL_CACHE")
set(HDF5_VOL_CACHE_PACKAGE_DESCRIPTION "HDF5 VOL connector for CACHE")
set(HDF5_VOL_CACHE_PACKAGE_URL "https://github.com/hpc-io/vol-cache/")
set(HDF5_VOL_CACHE_PACKAGE_VENDOR "HPC IO")
message(STATUS "Configuring ${HDF5_VOL_CACHE_PACKAGE} ${PROJECT_VERSION}")

#-----------------------------------------------------------------------------
# Source
#-----------------------------------------------------------------------------
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src
		    ${CMAKE_CURRENT_SOURCE_DIR}/utils)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/src)

#-----------------------------------------------------------------------------
# Testing
#-----------------------------------------------------------------------------
enable_testing()
include(CTest)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/tests)

