cmake_minimum_required(VERSION 3.12)


project(HDF5_VOL_CACHE VERSION 2.0)
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})

include(CTest)
enable_testing()

find_package(MPI REQUIRED)
find_package(ASYNC REQUIRED)
find_package(HDF5 REQUIRED COMPONENTS C)

include_directories(${MPI_INCLUDE_PATH})
include_directories(${HDF5_INCLUDE_DIRS})
include_directories(${ASYNC_INCLUDE_DIRS})

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
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/tests)

#-----------------------------------------------------------------------------
# Benchmarking
#-----------------------------------------------------------------------------
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/benchmarks)

#-----------------------------------------------------------------------------
# Utilities 
#-----------------------------------------------------------------------------
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/utils)
