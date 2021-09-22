if(DEFINED ENV{HDF5_PLUGIN_PATH})
  message("HDF5_PLUGIN_PATH is " $ENV{HDF5_PLUGIN_PATH})
endif()
find_package(MPI)
include_directories(SYSTEM ${MPI_INCLUDE_PATH})

set(test vpicio_uni_h5)

if(DEFINED ENV{HDF5_VOL_CONNECTOR})
  message($ENV{HDF5_VOL_CONNECTOR})
endif()

foreach(test ${tests})
  add_executable(${test}.exe ${CMAKE_CURRENT_SOURCE_DIR}/vpicio_hdf5/${test}.c)
  target_link_libraries(${test}.exe PRIVATE hdf5_vol_cache hdf5 ${MPI_C_LIBRARIES})
  add_test(${test} ${test}.exe)
  set_tests_properties(
    ${test}
    PROPERTIES
    ENVIRONMENT "HDF5_VOL_CONNECTOR=\"cache_ext config=config1.dat;under_vol=0;under_info={};\";HDF5_PLUGIN_PATH=$ENV{HDF5_PLUGIN_PATH}")
endforeach ()

