#include "H5VL.h"
herr H5LSset(LocalStorage *LS, local_storage_type type, char *path, hsize_t avail_space)
{
  LS->type = type;
  LS->avail_space = avail_space;
  LS->path = path;//check existence of the space
  return 0; 
}

herr H5LSclaim_space(LocalStorage *LS, hsize_t size, char *folder, claim_type type) {
  if (LS->avail_space > size) {
    LS->avail_space = LS->avail_space - size;
    return SUCCEED;
  } else {
    if (type == SOFT || LS->total_space < size) {
      return FAIL; 
    } else if {
      
    }
  }
}
