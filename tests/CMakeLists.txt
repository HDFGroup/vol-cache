if(DEFINED ENV{HDF5_PLUGIN_PATH})
  message("HDF5_PLUGIN_PATH is " $ENV{HDF5_PLUGIN_PATH})
endif()

find_package(MPI)
include_directories(SYSTEM ${MPI_INCLUDE_PATH})
include_directories(${HDF5_INCLUDE_DIRS})
include_directories(${ASYNC_INCLUDE_DIRS})

set(tests test_file test_group test_dataset test_dataset_async_api test_write_multi test_multdset)

file(COPY config_1.cfg DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

# Set up the environment for the test run.
list(
    APPEND
    TEST_ENV
    "HDF5_VOL_CONNECTOR=cache_ext config=config_1.cfg\\;under_vol=0\\;under_info={}"
)
message("HDF5_VOL_CONNECTOR is " ${TEST_ENV})

list(
    APPEND
    TEST_ENV
    "HDF5_PLUGIN_PATH=$ENV{HDF5_PLUGIN_PATH}"
)

foreach(test ${tests})
  add_executable(${test}.exe ${CMAKE_CURRENT_SOURCE_DIR}/${test}.cpp)
  target_link_libraries(${test}.exe PRIVATE ${MPI_C_LIBRARIES} ${HDF5_LIBRARIES} cache_new_h5api)
  add_test(${test} ${test}.exe)
  set_tests_properties(
    ${test}
    PROPERTIES
    ENVIRONMENT "${TEST_ENV}")
endforeach ()

install(
  TARGETS
    test_file.exe
    test_group.exe
    test_dataset.exe
    test_dataset_async_api.exe
    test_write_multi.exe
    test_multdset.exe
  RUNTIME DESTINATION ${HDF5_VOL_CACHE_INSTALL_BIN_DIR}
)
