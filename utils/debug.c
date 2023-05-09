/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (c) 2023, UChicago Argonne, LLC.                                *
 * All Rights Reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5 Cache VOL connector.  The full copyright notice *
 * terms governing use, modification, and redistribution, is contained in    *
 * the LICENSE file, which can be found at the root of the source code       *
 * distribution tree.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

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

int log_level() {
  if (getenv("HDF5_CACHE_LOG") != NULL)
    return atoi(getenv("HDF5_CACHE_LOG"));
  else
    return 0;
}
