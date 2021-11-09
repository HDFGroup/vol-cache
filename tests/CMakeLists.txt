if(DEFINED ENV{HDF5_PLUGIN_PATH})
  message("HDF5_PLUGIN_PATH is " $ENV{HDF5_PLUGIN_PATH})
endif()

if(DEFINED ENV{HDF5_VOL_CONNECTOR})
  message("HDF5_VOL_CONNECTOR is " $ENV{HDF5_VOL_CONNECTOR})
endif()

find_package(MPI)
include_directories(SYSTEM ${MPI_INCLUDE_PATH})

set(tests test_file test_group test_dataset test_dataset_async_api)

foreach(test ${tests})
  add_executable(${test}.exe ${CMAKE_CURRENT_SOURCE_DIR}/${test}.cpp)
  target_link_libraries(${test}.exe PRIVATE hdf5_vol_cache hdf5 ${MPI_C_LIBRARIES})
  add_test(${test} ${test}.exe)
  set_tests_properties(
    ${test}
    PROPERTIES
    ENVIRONMENT "HDF5_VOL_CONNECTOR=\"cache_ext config=config_1.cfg;under_vol=0;under_info={};\";HDF5_PLUGIN_PATH=$ENV{HDF5_PLUGIN_PATH}")
endforeach ()

