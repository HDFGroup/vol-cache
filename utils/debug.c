#include "stdio.h"
#include "stdlib.h"
int debug_level() {
  if (getenv("DEBUG") != NULL)
    return atoi(getenv("DEBUG"));
  else
    return 0;
}

int io_node() {
  if (getenv("IO_NODE") != NULL)
    return atof(getenv("IO_NODE"));
  else
    return 0;
}
