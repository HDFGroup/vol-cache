#include "cache_new_h5api.h"
#include <assert.h>

/* Operation values for new "API" routines */
/* These are initialized in the VOL connector's 'init' callback at runtime.                                                                     
 *      It's good practice to reset them back to -1 in the 'term' callback.                                                                     
 */
static int H5VL_new_api_dataset_foo_op_g = -1;
static int H5VL_new_api_dataset_bar_op_g = -1;
static int H5VL_new_api_group_fiddle_op_g = -1;
static int H5VL_new_api_dataset_prefetch_op_g = -1;  
static int H5VL_new_api_dataset_read_to_cache_op_g = -1;  
static int H5VL_new_api_dataset_read_from_cache_op_g = -1;
static int H5VL_new_api_dataset_mmap_remap_op_g = -1;

static int H5VL_new_api_dataset_cache_create_op_g = -1;
static int H5VL_new_api_dataset_cache_remove_op_g = -1; 

static int H5VL_new_api_file_cache_create_op_g = -1; // this is for reserving cache space for the file
static int H5VL_new_api_file_cache_remove_op_g = -1; //


/*-------------------------------------------------------------------------
 * Function:    H5Dmmap_remap
 *
 * Purpose:     free, munmap the mmap and recreate mmap.  
 *
 * Return:      Success:    0
 *              Failure:    -1
 * Comment:    This is mainly for removing cache effect. Only works in some system.  
 *-------------------------------------------------------------------------
 */
herr_t 
H5Dmmap_remap(hid_t dset_id) {
  if(-1 == H5VL_new_api_dataset_mmap_remap_op_g)
    if(H5VLfind_opt_operation(H5VL_SUBCLS_DATASET, H5VL_CACHE_EXT_DYN_DMMAP_REMAP, &H5VL_new_api_dataset_mmap_remap_op_g) < 0)
      return(-1);

  if(H5VLdataset_optional_op(dset_id, H5VL_new_api_dataset_mmap_remap_op_g, H5P_DATASET_XFER_DEFAULT, NULL) < 0) 
    return (-1);
  return 0; 
}


/*-------------------------------------------------------------------------
 * Function:    H5Dfoo
 *
 * Purpose:     Performs the 'foo' operation on a dataset, using the
 *              dataset 'optional' VOL callback.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dfoo(hid_t dset_id, hid_t dxpl_id, void **req, int i, double d)
{
    /* Sanity check */
  if(-1 == H5VL_new_api_dataset_foo_op_g)
    if(H5VLfind_opt_operation(H5VL_SUBCLS_DATASET, H5VL_CACHE_EXT_DYN_DFOO, &H5VL_new_api_dataset_foo_op_g) < 0)
      return(-1);

    /* Call the VOL dataset optional routine, requesting 'foo' occur */
    if(H5VLdataset_optional_op(dset_id, H5VL_new_api_dataset_foo_op_g, dxpl_id, req, i, d) < 0)
        return(-1);

    return 0;
} /* end H5Dfoo() */


/*-------------------------------------------------------------------------
 * Function:    H5Dread_to_cache
 *
 * Purpose:     Performs H5Dread and save the data to the local storage 
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t 
H5Dread_to_cache(hid_t dset_id, hid_t mem_type_id, hid_t mem_space_id,
    hid_t file_space_id, hid_t plist_id, void *buf) {
    void **req = NULL; 
    if(-1 == H5VL_new_api_dataset_read_to_cache_op_g)
      if(H5VLfind_opt_operation(H5VL_SUBCLS_DATASET, H5VL_CACHE_EXT_DYN_DREAD_TO_CACHE, &H5VL_new_api_dataset_read_to_cache_op_g) < 0)
	return(-1);

    if(H5VLdataset_optional_op(dset_id, H5VL_new_api_dataset_read_to_cache_op_g, plist_id, req, 
			       mem_type_id, mem_space_id, 
			       file_space_id, buf) < 0) 
      return (-1);
    return 0; 
} /* end H5Dread_to_cache ()*/

herr_t H5Dprefetch(hid_t dset_id, hid_t file_space_id, hid_t plist_id) {
  void **req = NULL;
  if(-1 == H5VL_new_api_dataset_prefetch_op_g)
    if(H5VLfind_opt_operation(H5VL_SUBCLS_DATASET, H5VL_CACHE_EXT_DYN_DPREFETCH, &H5VL_new_api_dataset_prefetch_op_g) < 0)
      return(-1);
  if (H5VLdataset_optional_op(dset_id, H5VL_new_api_dataset_prefetch_op_g, plist_id, req, file_space_id) < 0) 
    return (-1);
  return 0; 
} /* end H5Dpefetch() */

/*-------------------------------------------------------------------------
 * Function:    H5Dread_from_cache
 *
 * Purpose:     Performs reading dataset from the local storage 
 *
 * Return:      Success:    0
 *              Failure:    -1
 * Comment: 
 *    Notice that H5Dread_to_cache must be called before H5Dread_from_cache,
 *     Otherwise random data will be read. 
 *-------------------------------------------------------------------------
 */
herr_t 
H5Dread_from_cache(hid_t dset_id, hid_t mem_type_id, hid_t mem_space_id,
		     hid_t file_space_id, hid_t plist_id, void *buf) {
  void **req = NULL; 
  if(-1 == H5VL_new_api_dataset_read_from_cache_op_g)
    if(H5VLfind_opt_operation(H5VL_SUBCLS_DATASET, H5VL_CACHE_EXT_DYN_DREAD_FROM_CACHE, &H5VL_new_api_dataset_read_from_cache_op_g) < 0)
      return(-1);
  
  if(H5VLdataset_optional_op(dset_id, H5VL_new_api_dataset_read_from_cache_op_g, plist_id, req, 
			     mem_type_id, mem_space_id, 
			     file_space_id, buf) < 0) 
    return (-1);
  return 0; 
}



/*-------------------------------------------------------------------------
 * Function:    H5Dcache_remove
 *
 * Purpose:     Explicitly remove the cache related to the dataset.  
 *
 * Return:      Success:    0
 *              Failure:    -1
 * Comment:    
 *-------------------------------------------------------------------------
 */
herr_t 
H5Dcache_remove(hid_t dset_id) {
  if(-1 == H5VL_new_api_dataset_cache_remove_op_g)
    if(H5VLfind_opt_operation(H5VL_SUBCLS_DATASET, H5VL_CACHE_EXT_DYN_DCACHE_REMOVE, &H5VL_new_api_dataset_cache_remove_op_g) < 0)
      return(-1);
  
  if(H5VLdataset_optional_op(dset_id, H5VL_new_api_dataset_cache_remove_op_g, H5P_DATASET_XFER_DEFAULT, NULL) < 0) 
    return (-1);
  return 0; 
}

herr_t 
H5Dcache_create(hid_t dset_id, char *name) {
  if(-1 == H5VL_new_api_dataset_cache_create_op_g)
    if(H5VLfind_opt_operation(H5VL_SUBCLS_DATASET, H5VL_CACHE_EXT_DYN_DCACHE_CREATE, &H5VL_new_api_dataset_cache_create_op_g) < 0)
      return(-1);
  
  if(H5VLdataset_optional_op(dset_id, H5VL_new_api_dataset_cache_create_op_g, H5P_DATASET_XFER_DEFAULT, NULL, name) < 0) 
    return (-1);
  return 0; 
}

herr_t
H5Fcache_create(hid_t file_id, hid_t dapl_id, hsize_t size, cache_purpose_t purpose, cache_duration_t duration) {
  /* Sanity check */
  if(-1 == H5VL_new_api_file_cache_remove_op_g)
    if(H5VLfind_opt_operation(H5VL_SUBCLS_FILE, H5VL_CACHE_EXT_DYN_FCACHE_CREATE, &H5VL_new_api_file_cache_create_op_g) < 0)
      return(-1);

  /* Call the VOL file optional routine */
  if (H5VLfile_optional_op(file_id, H5VL_new_api_file_cache_create_op_g,
			   H5P_DATASET_XFER_DEFAULT, NULL,
			   dapl_id, size, purpose, duration) < 0)
    return (-1);
  return 0; 
}

herr_t
H5Fcache_remove(hid_t file_id) {
  /* Sanity check */

  if(-1 == H5VL_new_api_file_cache_remove_op_g)
    if(H5VLfind_opt_operation(H5VL_SUBCLS_FILE, H5VL_CACHE_EXT_DYN_FCACHE_REMOVE, &H5VL_new_api_file_cache_remove_op_g) < 0)
      return(-1);
  /* Call the VOL file optional routine */
  if (H5VLfile_optional_op(file_id, H5VL_new_api_file_cache_remove_op_g, H5P_DATASET_XFER_DEFAULT, NULL) < 0)
    return (-1);

  return 0; 
}


/*-------------------------------------------------------------------------
 * Function:    H5Dbar
 *
 * Purpose:     Performs the 'bar' operation on a dataset, using the
 *              dataset 'optional' VOL callback.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dbar(hid_t dset_id, hid_t dxpl_id, void **req, double *dp, unsigned *up)
{
    /* Sanity check */

    if(-1 == H5VL_new_api_dataset_bar_op_g)
    if(H5VLfind_opt_operation(H5VL_SUBCLS_DATASET, H5VL_CACHE_EXT_DYN_DBAR, &H5VL_new_api_dataset_bar_op_g) < 0)
      return(-1);

    /* Call the VOL dataset optional routine, requesting 'bar' occur */
    if(H5VLdataset_optional_op(dset_id, H5VL_new_api_dataset_bar_op_g, dxpl_id, req, dp, up) < 0)
        return(-1);

    return 0;
} /* end H5Dbar() */


/*-------------------------------------------------------------------------
 * Function:    H5Gfiddle
 *
 * Purpose:     Performs the 'fiddle' operation on a group, using the
 *              group 'optional' VOL callback.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Gfiddle(hid_t dset_id, hid_t dxpl_id, void **req)
{
    /* Sanity check */
  if(-1 == H5VL_new_api_dataset_cache_remove_op_g)
    if(H5VLfind_opt_operation(H5VL_SUBCLS_GROUP, H5VL_CACHE_EXT_DYN_GFIDDLE, &H5VL_new_api_group_fiddle_op_g) < 0)
      return(-1);
    /* Call the VOL group optional routine, requesting 'fiddle' occur */
    if(H5VLgroup_optional_op(dset_id, H5VL_new_api_group_fiddle_op_g, dxpl_id, req) < 0)
        return(-1);

    return 0;
} /* end H5Gfiddle() */

