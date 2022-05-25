find_path(ASYNC_INCLUDE_DIR
    NAMES h5_async_vol.h h5_async_lib.h
    HINTS $ENV{HDF5_VOL_DIR}
    PATH_SUFFIXES include
)

find_library(ASYNC_HDF5_LIBRARY
    NAMES asynchdf5 
    HINTS $ENV{HDF5_VOL_DIR}
    PATH_SUFFIXES lib
)

find_library(H5_ASYNC_LIBRARY
    NAMES h5async
    HINTS $ENV{HDF5_VOL_DIR}
    PATH_SUFFIXES lib
)

set(ASYNC_INCLUDE_DIRS ${ASYNC_INCLUDE_DIR})
set(ASYNC_LIBRARIES ${ASYNC_HDF5_LIBRARY})

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(
    ASYNC
    DEFAULT_MSG
    ASYNC_LIBRARIES
    ASYNC_INCLUDE_DIRS
)

mark_as_advanced(
    ASYNC_HDF5_LIBRARY
    ASYNC_INCLUDE_DIR
)


