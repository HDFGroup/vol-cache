#include "stdio.h"
#include "stdlib.h"
int debug_level() {
  if (getenv("HDF5_CACHE_DEBUG") != NULL)
    return atoi(getenv("HDF5_CACHE_DEBUG"));
  else
    return 0;
}

int io_node() {
  if (getenv("IO_NODE") != NULL)
    return atof(getenv("IO_NODE"));
  else
    return 0;
}
