#define NEW_H5API_IMPL
#include "cache_new_h5api.h"
#include "H5VLcache_ext_private.h"
#include <assert.h>

/* Operation values for new "API" routines */
/* These are initialized in the VOL connector's 'init' callback at runtime.
 *      It's good practice to reset them back to -1 in the 'term' callback.
 */


static int H5VL_new_api_dataset_prefetch_op_g = -1;
static int H5VL_new_api_dataset_read_to_cache_op_g = -1;
static int H5VL_new_api_dataset_read_from_cache_op_g = -1;
static int H5VL_new_api_dataset_mmap_remap_op_g = -1;
static int H5VL_new_api_dataset_cache_create_op_g = -1;
static int H5VL_new_api_dataset_cache_remove_op_g = -1;
static int H5VL_new_api_file_cache_create_op_g = -1; // this is for reserving cache space for the file
static int H5VL_new_api_file_cache_remove_op_g = -1; //
static int H5VL_new_api_file_async_op_pause_op_g = -1;
static int H5VL_new_api_file_async_op_start_op_g = -1;
static void
cache_ext_reset(void *_ctx)
{
  H5VL_new_api_dataset_prefetch_op_g = -1;
  H5VL_new_api_dataset_read_to_cache_op_g = -1;
  H5VL_new_api_dataset_read_from_cache_op_g = -1;
  H5VL_new_api_dataset_mmap_remap_op_g = -1;

  H5VL_new_api_dataset_cache_create_op_g = -1;
  H5VL_new_api_dataset_cache_remove_op_g = -1;

  H5VL_new_api_file_cache_create_op_g = -1; // this is for reserving cache space for the file
  H5VL_new_api_file_cache_remove_op_g = -1; //
  H5VL_new_api_file_async_op_pause_op_g = -1;
  H5VL_new_api_file_async_op_start_op_g = -1; 
}

static int
cache_ext_setup(void)
{
  if(H5VLfind_opt_operation(H5VL_SUBCLS_DATASET, H5VL_CACHE_EXT_DYN_DMMAP_REMAP, &H5VL_new_api_dataset_mmap_remap_op_g) < 0)
    return(-1);
  if(H5VLfind_opt_operation(H5VL_SUBCLS_DATASET, H5VL_CACHE_EXT_DYN_DREAD_TO_CACHE, &H5VL_new_api_dataset_read_to_cache_op_g) < 0)
    return(-1);
  if(H5VLfind_opt_operation(H5VL_SUBCLS_DATASET, H5VL_CACHE_EXT_DYN_DPREFETCH, &H5VL_new_api_dataset_prefetch_op_g) < 0)
    return(-1);
  if(H5VLfind_opt_operation(H5VL_SUBCLS_DATASET, H5VL_CACHE_EXT_DYN_DREAD_FROM_CACHE, &H5VL_new_api_dataset_read_from_cache_op_g) < 0)
      return(-1);
  if(H5VLfind_opt_operation(H5VL_SUBCLS_DATASET, H5VL_CACHE_EXT_DYN_DCACHE_REMOVE, &H5VL_new_api_dataset_cache_remove_op_g) < 0)
      return(-1);
  if(H5VLfind_opt_operation(H5VL_SUBCLS_DATASET, H5VL_CACHE_EXT_DYN_DCACHE_CREATE, &H5VL_new_api_dataset_cache_create_op_g) < 0)
    return(-1);

  if(H5VLfind_opt_operation(H5VL_SUBCLS_FILE, H5VL_CACHE_EXT_DYN_FCACHE_CREATE, &H5VL_new_api_file_cache_create_op_g) < 0)
      return(-1);
  if(H5VLfind_opt_operation(H5VL_SUBCLS_FILE, H5VL_CACHE_EXT_DYN_FCACHE_REMOVE, &H5VL_new_api_file_cache_remove_op_g) < 0)
    return(-1);

  if(H5VLfind_opt_operation(H5VL_SUBCLS_FILE, H5VL_CACHE_EXT_DYN_FASYNC_OP_PAUSE, &H5VL_new_api_file_async_op_pause_op_g) < 0)
    return(-1);

  if(H5VLfind_opt_operation(H5VL_SUBCLS_FILE, H5VL_CACHE_EXT_DYN_FASYNC_OP_START, &H5VL_new_api_file_async_op_start_op_g) < 0)
    return(-1);

  /* Register callback for library shutdown, to release resources */
  if (H5atclose(cache_ext_reset, NULL) < 0) {
    fprintf(stderr, "H5atclose failed\n");
    return(-1);
  }

  return 0;
}


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
H5Dmmap_remap(const char *app_file, const char *app_func, unsigned app_line, hid_t dset_id)
{
    H5VL_optional_args_t vol_cb_args;                   /* Wrapper for invoking optional operation */

    if(cache_ext_setup() < 0)
        return(-1);
    assert(H5VL_new_api_dataset_mmap_remap_op_g > 0);

    /* Set up args for invoking optional callback */
    vol_cb_args.op_type = H5VL_new_api_dataset_mmap_remap_op_g;
    vol_cb_args.args = NULL;

    if(H5VLdataset_optional_op_wrap(app_file, app_func, app_line, dset_id, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5ES_NONE) < 0)
        return (-1);

    return 0;
}


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
H5Dread_to_cache(const char *app_file, const char *app_func, unsigned app_line, hid_t dset_id, hid_t mem_type_id, hid_t mem_space_id,
    hid_t file_space_id, hid_t plist_id, void *buf)
{
    H5VL_optional_args_t vol_cb_args;                   /* Wrapper for invoking optional operation */
    H5VL_cache_ext_dataset_read_to_cache_args_t opt_args;

    if(cache_ext_setup() < 0)
      return(-1);
    assert(H5VL_new_api_dataset_read_to_cache_op_g>0);

    /* Set up args for invoking optional callback */
    opt_args.mem_type_id = mem_type_id;
    opt_args.mem_space_id = mem_space_id;
    opt_args.file_space_id = file_space_id;
    opt_args.buf = buf;
    vol_cb_args.op_type = H5VL_new_api_dataset_read_to_cache_op_g;
    vol_cb_args.args = &opt_args;

    if(H5VLdataset_optional_op_wrap(app_file, app_func, app_line, dset_id, &vol_cb_args, plist_id, H5ES_NONE) < 0)
      return (-1);

    return 0;
} /* end H5Dread_to_cache ()*/


/*-------------------------------------------------------------------------
 * Function:    H5Dread_to_cache_async
 *
 * Purpose:     Performs H5Dread and save the data to the local storage
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dread_to_cache_async(const char *app_file, const char *app_func, unsigned app_line, hid_t dset_id, hid_t mem_type_id, hid_t mem_space_id,
		       hid_t file_space_id, hid_t plist_id, void *buf, hid_t es_id)
{
    H5VL_optional_args_t vol_cb_args;                   /* Wrapper for invoking optional operation */
    H5VL_cache_ext_dataset_read_to_cache_args_t opt_args;

    if(cache_ext_setup() < 0)
      return(-1);
    assert(H5VL_new_api_dataset_read_to_cache_op_g>0);

    /* Set up args for invoking optional callback */
    opt_args.mem_type_id = mem_type_id;
    opt_args.mem_space_id = mem_space_id;
    opt_args.file_space_id = file_space_id;
    opt_args.buf = buf;
    vol_cb_args.op_type = H5VL_new_api_dataset_read_to_cache_op_g;
    vol_cb_args.args = &opt_args;

    if(H5VLdataset_optional_op_wrap(app_file, app_func, app_line, dset_id, &vol_cb_args, plist_id, es_id) < 0)
      return (-1);

    return 0;
} /* end H5Dread_to_cache_async ()*/


/*-------------------------------------------------------------------------
 * Function:    H5Dprefetch
 *
 * Purpose:     Prefetch the data to the local storage
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5Dprefetch(const char *app_file, const char *app_func, unsigned app_line, hid_t dset_id, hid_t file_space_id, hid_t plist_id)
{
    H5VL_optional_args_t vol_cb_args;                   /* Wrapper for invoking optional operation */
    H5VL_cache_ext_dataset_prefetch_args_t opt_args;

    if(cache_ext_setup() < 0)
        return(-1);
    assert(0 < H5VL_new_api_dataset_prefetch_op_g);

    /* Set up args for invoking optional callback */
    opt_args.file_space_id = file_space_id;
    vol_cb_args.op_type = H5VL_new_api_dataset_prefetch_op_g;
    vol_cb_args.args = &opt_args;

    if (H5VLdataset_optional_op_wrap(app_file, app_func, app_line, dset_id, &vol_cb_args, plist_id, H5ES_NONE) < 0)
        return (-1);

    return 0;
} /* end H5Dprefetch() */


/*-------------------------------------------------------------------------
 * Function:    H5Dprefetch_async
 *
 * Purpose:     Asychronously prefetch the data to the local storage
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5Dprefetch_async(const char *app_file, const char *app_func, unsigned app_line, hid_t dset_id, hid_t file_space_id, hid_t plist_id, hid_t es_id)
{
    H5VL_optional_args_t vol_cb_args;                   /* Wrapper for invoking optional operation */
    H5VL_cache_ext_dataset_prefetch_args_t opt_args;

    if(cache_ext_setup() < 0)
        return(-1);
    assert(0 < H5VL_new_api_dataset_prefetch_op_g);

    /* Set up args for invoking optional callback */
    opt_args.file_space_id = file_space_id;
    vol_cb_args.op_type = H5VL_new_api_dataset_prefetch_op_g;
    vol_cb_args.args = &opt_args;

    if (H5VLdataset_optional_op_wrap(app_file, app_func, app_line, dset_id, &vol_cb_args, plist_id, es_id) < 0)
        return (-1);

    return 0;
} /* end H5Dpefetch_asyc() */



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
H5Dread_from_cache(const char *app_file, const char *app_func, unsigned app_line, hid_t dset_id, hid_t mem_type_id, hid_t mem_space_id,
		     hid_t file_space_id, hid_t plist_id, void *buf)
{
    H5VL_optional_args_t vol_cb_args;                   /* Wrapper for invoking optional operation */
    H5VL_cache_ext_dataset_read_from_cache_args_t opt_args;

    if(cache_ext_setup() < 0)
        return(-1);
    assert(0 < H5VL_new_api_dataset_read_from_cache_op_g);

    /* Set up args for invoking optional callback */
    opt_args.mem_type_id = mem_type_id;
    opt_args.mem_space_id = mem_space_id;
    opt_args.file_space_id = file_space_id;
    opt_args.buf = buf;
    vol_cb_args.op_type = H5VL_new_api_dataset_read_from_cache_op_g;
    vol_cb_args.args = &opt_args;

    if(H5VLdataset_optional_op_wrap(app_file, app_func, app_line, dset_id, &vol_cb_args, plist_id, H5ES_NONE) < 0)
        return (-1);

    return 0;
}


/*-------------------------------------------------------------------------
 * Function:    H5Dread_from_cache_async
 *
 * Purpose:     Asynchronously performs reading dataset from the local storage
 *
 * Return:      Success:    0
 *              Failure:    -1
 * Comment:
 *    Notice that H5Dread_to_cache must be called before H5Dread_from_cache,
 *     Otherwise random data will be read.
 *-------------------------------------------------------------------------
 */
herr_t
H5Dread_from_cache_async(const char *app_file, const char *app_func, unsigned app_line, hid_t dset_id, hid_t mem_type_id, hid_t mem_space_id,
			 hid_t file_space_id, hid_t plist_id, void *buf, hid_t es_id)
{
    H5VL_optional_args_t vol_cb_args;                   /* Wrapper for invoking optional operation */
    H5VL_cache_ext_dataset_read_from_cache_args_t opt_args;

    if(cache_ext_setup() < 0)
        return(-1);
    assert(0 < H5VL_new_api_dataset_read_from_cache_op_g);

    /* Set up args for invoking optional callback */
    opt_args.mem_type_id = mem_type_id;
    opt_args.mem_space_id = mem_space_id;
    opt_args.file_space_id = file_space_id;
    opt_args.buf = buf;
    vol_cb_args.op_type = H5VL_new_api_dataset_read_from_cache_op_g;
    vol_cb_args.args = &opt_args;

    if(H5VLdataset_optional_op_wrap(app_file, app_func, app_line, dset_id, &vol_cb_args, plist_id, es_id) < 0)
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
H5Dcache_remove(const char *app_file, const char *app_func, unsigned app_line, hid_t dset_id)
{
    H5VL_optional_args_t vol_cb_args;                   /* Wrapper for invoking optional operation */

    if (cache_ext_setup()<0)
        return (-1);
    assert(0< H5VL_new_api_dataset_cache_remove_op_g);

    /* Set up args for invoking optional callback */
    vol_cb_args.op_type = H5VL_new_api_dataset_cache_remove_op_g;
    vol_cb_args.args = NULL;

    if(H5VLdataset_optional_op_wrap(app_file, app_func, app_line, dset_id, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5ES_NONE) < 0)
        return (-1);

    return 0;
}


/*-------------------------------------------------------------------------
 * Function:    H5Dcache_remove_async
 *
 * Purpose:     Asychronously remove the cache related to the dataset.
 *
 * Return:      Success:    0
 *              Failure:    -1
 * Comment:
 *-------------------------------------------------------------------------
 */
herr_t
H5Dcache_remove_async(const char *app_file, const char *app_func, unsigned app_line, hid_t dset_id, hid_t es_id)
{
    H5VL_optional_args_t vol_cb_args;                   /* Wrapper for invoking optional operation */

    if (cache_ext_setup()<0)
        return (-1);
    assert(0< H5VL_new_api_dataset_cache_remove_op_g);

    /* Set up args for invoking optional callback */
    vol_cb_args.op_type = H5VL_new_api_dataset_cache_remove_op_g;
    vol_cb_args.args = NULL;

    if(H5VLdataset_optional_op_wrap(app_file, app_func, app_line, dset_id, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, es_id) < 0)
        return (-1);

    return 0;
}


/*-------------------------------------------------------------------------
 * Function:    H5Dcache_create (still experimental)
 *
 * Purpose:     Create cache for a dataset
 *
 * Return:      Success:    0
 *              Failure:    -1
 * Comment:
 *-------------------------------------------------------------------------
 */
herr_t
H5Dcache_create(const char *app_file, const char *app_func, unsigned app_line, hid_t dset_id, const char *name)
{
    H5VL_optional_args_t vol_cb_args;                   /* Wrapper for invoking optional operation */
    H5VL_cache_ext_dataset_cache_create_args_t opt_args;

    if (cache_ext_setup()<0)
        return (-1);
    assert(0< H5VL_new_api_dataset_cache_create_op_g);

    /* Set up args for invoking optional callback */
    opt_args.name = name;
    vol_cb_args.op_type = H5VL_new_api_dataset_cache_create_op_g;
    vol_cb_args.args = &opt_args;

    if(H5VLdataset_optional_op_wrap(app_file, app_func, app_line, dset_id, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5ES_NONE) < 0)
        return (-1);

    return 0;
}


/*-------------------------------------------------------------------------
 * Function:    H5Dcache_create_async (still experimental)
 *
 * Purpose:     Asychronously create cache for a dataset
 *
 * Return:      Success:    0
 *              Failure:    -1
 * Comment:
 *-------------------------------------------------------------------------
 */
herr_t
H5Dcache_create_async(const char *app_file, const char *app_func, unsigned app_line, hid_t dset_id, char *name, hid_t es_id)
{
    H5VL_optional_args_t vol_cb_args;                   /* Wrapper for invoking optional operation */
    H5VL_cache_ext_dataset_cache_create_args_t opt_args;

    if (cache_ext_setup()<0)
        return (-1);
    assert(0< H5VL_new_api_dataset_cache_create_op_g);

    /* Set up args for invoking optional callback */
    opt_args.name = name;
    vol_cb_args.op_type = H5VL_new_api_dataset_cache_create_op_g;
    vol_cb_args.args = &opt_args;

    if(H5VLdataset_optional_op_wrap(app_file, app_func, app_line, dset_id, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, es_id) < 0)
        return (-1);

    return 0;
}


/*-------------------------------------------------------------------------
 * Function:    H5Fcache_create
 *
 * Purpose:     Create cache for a file
 *
 * Return:      Success:    0
 *              Failure:    -1
 * Comment:
 *-------------------------------------------------------------------------
 */
herr_t
H5Fcache_create(const char *app_file, const char *app_func, unsigned app_line, hid_t file_id, hid_t fapl_id, hsize_t size, cache_purpose_t purpose, cache_duration_t duration)
{
    H5VL_optional_args_t vol_cb_args;                   /* Wrapper for invoking optional operation */
    H5VL_cache_ext_file_cache_create_args_t opt_args;

    /* Sanity check */
    if (cache_ext_setup()<0)
        return (-1);
    assert(0 < H5VL_new_api_file_cache_create_op_g);

    /* Set up args for invoking optional callback */
    opt_args.fapl_id = fapl_id;
    opt_args.size = size;
    opt_args.purpose = purpose;
    opt_args.duration = duration;
    vol_cb_args.op_type = H5VL_new_api_file_cache_create_op_g;
    vol_cb_args.args = &opt_args;

    /* Call the VOL file optional routine */
    if (H5VLfile_optional_op_wrap(app_file, app_func, app_line, file_id, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5ES_NONE) < 0)
        return (-1);

    return 0;
}


/*-------------------------------------------------------------------------
 * Function:    H5Fcache_create_async
 *
 * Purpose:     Asychronously create cache for a file
 *
 * Return:      Success:    0
 *              Failure:    -1
 * Comment:
 *-------------------------------------------------------------------------
 */
herr_t
H5Fcache_create_async(const char *app_file, const char *app_func, unsigned app_line, hid_t file_id, hid_t fapl_id, hsize_t size, cache_purpose_t purpose, cache_duration_t duration, hid_t es_id)
{
    H5VL_optional_args_t vol_cb_args;                   /* Wrapper for invoking optional operation */
    H5VL_cache_ext_file_cache_create_args_t opt_args;

    /* Sanity check */
    if (cache_ext_setup()<0)
        return (-1);
    assert(0 < H5VL_new_api_file_cache_create_op_g);

    /* Set up args for invoking optional callback */
    opt_args.fapl_id = fapl_id;
    opt_args.size = size;
    opt_args.purpose = purpose;
    opt_args.duration = duration;
    vol_cb_args.op_type = H5VL_new_api_file_cache_create_op_g;
    vol_cb_args.args = &opt_args;

    /* Call the VOL file optional routine */
    if (H5VLfile_optional_op_wrap(app_file, app_func, app_line, file_id, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, es_id) < 0)
        return (-1);

    return 0;
}


/*-------------------------------------------------------------------------
 * Function:    H5Fcache_remove
 *
 * Purpose:     Remove cache for a file
 *
 * Return:      Success:    0
 *              Failure:    -1
 * Comment:
 *-------------------------------------------------------------------------
 */
herr_t
H5Fcache_remove(const char *app_file, const char *app_func, unsigned app_line, hid_t file_id)
{
    H5VL_optional_args_t vol_cb_args;                   /* Wrapper for invoking optional operation */

    if(cache_ext_setup() < 0)
        return(-1);
    assert(0 < H5VL_new_api_file_cache_remove_op_g);

    /* Set up args for invoking optional callback */
    vol_cb_args.op_type = H5VL_new_api_file_cache_remove_op_g;
    vol_cb_args.args = NULL;

    if (H5VLfile_optional_op_wrap(app_file, app_func, app_line, file_id, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5ES_NONE) < 0)
        return (-1);

    return 0;
}


/*-------------------------------------------------------------------------
 * Function:    H5Fasync_op_start
 *
 * Purpose:     Start all the async operations associate with the file. 
 *
 * Return:      Success:    0
 *              Failure:    -1
 * Comment:
 *-------------------------------------------------------------------------
 */
herr_t
H5Fasync_op_start(const char *app_file, const char *app_func, unsigned app_line, hid_t file_id)
{
    H5VL_optional_args_t vol_cb_args;                   /* Wrapper for invoking optional operation */

    if(cache_ext_setup() < 0)
        return(-1);
    assert(0 < H5VL_new_api_file_async_op_start_op_g);

    /* Set up args for invoking optional callback */
    vol_cb_args.op_type = H5VL_new_api_file_async_op_start_op_g;
    vol_cb_args.args = NULL;

    if (H5VLfile_optional_op_wrap(app_file, app_func, app_line, file_id, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5ES_NONE) < 0)
        return (-1);

    return 0;
}



/*-------------------------------------------------------------------------
 * Function:    H5Fasync_op_pause
 *
 * Purpose:     Pause all the async operations associate with the file. 
 *
 * Return:      Success:    0
 *              Failure:    -1
 * Comment:
 *-------------------------------------------------------------------------
 */
herr_t
H5Fasync_op_pause(const char *app_file, const char *app_func, unsigned app_line, hid_t file_id)
{
    H5VL_optional_args_t vol_cb_args;                   /* Wrapper for invoking optional operation */
    
    if(cache_ext_setup() < 0)
        return(-1);
    assert(0 < H5VL_new_api_file_async_op_pause_op_g);

    /* Set up args for invoking optional callback */
    vol_cb_args.op_type = H5VL_new_api_file_async_op_pause_op_g;
    vol_cb_args.args = NULL;

    if (H5VLfile_optional_op_wrap(app_file, app_func, app_line, file_id, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5ES_NONE) < 0)
        return (-1);

    return 0;
}



/*-------------------------------------------------------------------------
 * Function:    H5Fcache_remove_async
 *
 * Purpose:     Asychronously remove cache for a file
 *
 * Return:      Success:    0
 *              Failure:    -1
 * Comment:
 *-------------------------------------------------------------------------
 */
herr_t
H5Fcache_remove_async(const char *app_file, const char *app_func, unsigned app_line, hid_t file_id, hid_t es_id)
{
    H5VL_optional_args_t vol_cb_args;                   /* Wrapper for invoking optional operation */

    if(cache_ext_setup() < 0)
        return(-1);
    assert(0 < H5VL_new_api_file_cache_remove_op_g);

    /* Set up args for invoking optional callback */
    vol_cb_args.op_type = H5VL_new_api_file_cache_remove_op_g;
    vol_cb_args.args = NULL;

    if (H5VLfile_optional_op_wrap(app_file, app_func, app_line, file_id, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5ES_NONE) < 0)
        return (-1);

    return 0;
}

