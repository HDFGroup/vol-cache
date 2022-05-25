if(DEFINED ENV{HDF5_PLUGIN_PATH})
  message("HDF5_PLUGIN_PATH is " $ENV{HDF5_PLUGIN_PATH})
endif()

if(DEFINED ENV{HDF5_VOL_CONNECTOR})
  message("HDF5_VOL_CONNECTOR is " $ENV{HDF5_VOL_CONNECTOR})
endif()

find_package(MPI)
include_directories(SYSTEM ${MPI_INCLUDE_PATH})
include_directories(${HDF5_INCLUDE_DIRS})
include_directories(${ASYNC_INCLUDE_DIRS})

set(benchmarks write_cache write prepare_dataset read_cache)

foreach(test ${benchmarks})
  add_executable(${test}.exe ${CMAKE_CURRENT_SOURCE_DIR}/${test}.cpp ${CMAKE_CURRENT_SOURCE_DIR}/../utils/debug.c ${CMAKE_CURRENT_SOURCE_DIR}/profiling.cpp ${CMAKE_CURRENT_SOURCE_DIR}/../src/cache_utils.c)
  target_link_libraries(${test}.exe PRIVATE ${MPI_C_LIBRARIES} ${HDF5_LIBRARIES} cache_new_h5api)
  add_test(${test} ${test}.exe)
  set_tests_properties(
    ${test}
    PROPERTIES
    ENVIRONMENT "HDF5_VOL_CONNECTOR=\"cache_ext config=config_1.cfg;under_vol=0;under_info={};\";HDF5_PLUGIN_PATH=$ENV{HDF5_PLUGIN_PATH}")
endforeach ()

install(
  TARGETS
    write_cache.exe
    write.exe
    prepare_dataset.exe
    read_cache.exe
  RUNTIME DESTINATION ${HDF5_VOL_CACHE_INSTALL_BIN_DIR}
)
