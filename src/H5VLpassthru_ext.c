/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://support.hdfgroup.org/ftp/HDF5/releases.  *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Purpose:     This is a "pass through" VOL connector, which forwards each
 *              VOL callback to an underlying connector.
 *
 *              It is designed as an example VOL connector for developers to
 *              use when creating new connectors, especially connectors that
 *              are outside of the HDF5 library.  As such, it should _NOT_
 *              include _any_ private HDF5 header files.  This connector should
 *              therefore only make public HDF5 API calls and use standard C /
 *              POSIX calls.
 *
 *              Note that the HDF5 error stack must be preserved on code paths
 *              that could be invoked when the underlying VOL connector's
 *              callback can fail.
 *
 */


/* Header files needed */
/* Do NOT include private HDF5 files here! */
#include <assert.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
/* Public HDF5 file */
#include "hdf5.h"
#include "H5Dio_cache.h"
/* This connector's header */
#include "H5VLpassthru_ext.h"
#include <pthread.h>
#include "mpi.h"
#include "stdlib.h"
#include "string.h"
#include "unistd.h"
// POSIX I/O
#include "sys/stat.h"
#include <fcntl.h>
#include "H5Dio_cache.h"
#include "H5LS.h"
// Memory map
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/statvfs.h>
//debug
#include "debug.h"
#define  PAGESIZE sysconf(_SC_PAGE_SIZE)
#ifndef SUCCEED
#define SUCCEED 0
#endif
#ifndef FAIL
#define FAIL -1
#endif
#ifndef H5_REQUEST_NULL
#define H5_REQUEST_NULL NULL
#endif

hsize_t HDF5_WRITE_CACHE_SIZE;
/**********/
/* Macros */
/**********/

/* Whether to display log messge when callback is invoked */
/* (Uncomment to enable) */
/* #define ENABLE_EXT_PASSTHRU_LOGGING */

/* Hack for missing va_copy() in old Visual Studio editions
 * (from H5win2_defs.h - used on VS2012 and earlier)
 */
#if defined(_WIN32) && defined(_MSC_VER) && (_MSC_VER < 1800)
#define va_copy(D,S)      ((D) = (S))
#endif

/************/
/* Typedefs */
/************/
void *H5Dread_pthread_func_vol(void *args); 
void *H5Dwrite_pthread_func_vol(void *args);
/********************* */
/* Function prototypes */
/********************* */
/* Helper routines */
static herr_t H5VL_pass_through_ext_file_specific_reissue(void *obj, hid_t connector_id,
    H5VL_file_specific_t specific_type, hid_t dxpl_id, void **req, ...);
static herr_t H5VL_pass_through_ext_request_specific_reissue(void *obj, hid_t connector_id,
    H5VL_request_specific_t specific_type, ...);
static herr_t H5VL_pass_through_ext_link_create_reissue(H5VL_link_create_type_t create_type,
    void *obj, const H5VL_loc_params_t *loc_params, hid_t connector_id,
    hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req, ...);
static H5VL_pass_through_ext_t *H5VL_pass_through_ext_new_obj(void *under_obj,
    hid_t under_vol_id);
static herr_t H5VL_pass_through_ext_free_obj(H5VL_pass_through_ext_t *obj);

/* "Management" callbacks */
static herr_t H5VL_pass_through_ext_init(hid_t vipl_id);
static herr_t H5VL_pass_through_ext_term(void);

/* VOL info callbacks */
static void *H5VL_pass_through_ext_info_copy(const void *info);
static herr_t H5VL_pass_through_ext_info_cmp(int *cmp_value, const void *info1, const void *info2);
static herr_t H5VL_pass_through_ext_info_free(void *info);
static herr_t H5VL_pass_through_ext_info_to_str(const void *info, char **str);
static herr_t H5VL_pass_through_ext_str_to_info(const char *str, void **info);

/* VOL object wrap / retrieval callbacks */
static void *H5VL_pass_through_ext_get_object(const void *obj);
static herr_t H5VL_pass_through_ext_get_wrap_ctx(const void *obj, void **wrap_ctx);
static void *H5VL_pass_through_ext_wrap_object(void *obj, H5I_type_t obj_type,
    void *wrap_ctx);
static void *H5VL_pass_through_ext_unwrap_object(void *obj);
static herr_t H5VL_pass_through_ext_free_wrap_ctx(void *obj);

/* Attribute callbacks */
static void *H5VL_pass_through_ext_attr_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req);
static void *H5VL_pass_through_ext_attr_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t aapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_pass_through_ext_attr_read(void *attr, hid_t mem_type_id, void *buf, hid_t dxpl_id, void **req);
static herr_t H5VL_pass_through_ext_attr_write(void *attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req);
static herr_t H5VL_pass_through_ext_attr_get(void *obj, H5VL_attr_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_pass_through_ext_attr_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_attr_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_pass_through_ext_attr_optional(void *obj, H5VL_attr_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_pass_through_ext_attr_close(void *attr, hid_t dxpl_id, void **req);

/* Dataset callbacks */
static void *H5VL_pass_through_ext_dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req);
static void *H5VL_pass_through_ext_dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_pass_through_ext_dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id,
                                    hid_t file_space_id, hid_t plist_id, void *buf, void **req);

static herr_t H5VL_pass_through_ext_dataset_read_from_cache(void *dset, hid_t mem_type_id, hid_t mem_space_id,
        hid_t file_space_id, hid_t plist_id, void *buf, void **req);
static herr_t H5VL_pass_through_ext_dataset_read_to_cache(void *dset, hid_t mem_type_id, hid_t mem_space_id,
        hid_t file_space_id, hid_t plist_id, void *buf, void **req);
static herr_t H5VL_pass_through_ext_dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req);
static herr_t H5VL_pass_through_ext_dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_pass_through_ext_dataset_specific(void *obj, H5VL_dataset_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_pass_through_ext_dataset_optional(void *obj, H5VL_dataset_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_pass_through_ext_dataset_close(void *dset, hid_t dxpl_id, void **req);

/* Datatype callbacks */
static void *H5VL_pass_through_ext_datatype_commit(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id, hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id, hid_t dxpl_id, void **req);
static void *H5VL_pass_through_ext_datatype_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t tapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_pass_through_ext_datatype_get(void *dt, H5VL_datatype_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_pass_through_ext_datatype_specific(void *obj, H5VL_datatype_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_pass_through_ext_datatype_optional(void *obj, H5VL_datatype_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_pass_through_ext_datatype_close(void *dt, hid_t dxpl_id, void **req);

/* File callbacks */
static void *H5VL_pass_through_ext_file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req);
static void *H5VL_pass_through_ext_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_pass_through_ext_file_get(void *file, H5VL_file_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_pass_through_ext_file_specific(void *file, H5VL_file_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_pass_through_ext_file_optional(void *file, H5VL_file_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_pass_through_ext_file_close(void *file, hid_t dxpl_id, void **req);

/* Group callbacks */
static void *H5VL_pass_through_ext_group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req);
static void *H5VL_pass_through_ext_group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_pass_through_ext_group_get(void *obj, H5VL_group_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_pass_through_ext_group_specific(void *obj, H5VL_group_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_pass_through_ext_group_optional(void *obj, H5VL_group_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_pass_through_ext_group_close(void *grp, hid_t dxpl_id, void **req);

/* Link callbacks */
static herr_t H5VL_pass_through_ext_link_create(H5VL_link_create_type_t create_type, void *obj, const H5VL_loc_params_t *loc_params, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_pass_through_ext_link_copy(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_pass_through_ext_link_move(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_pass_through_ext_link_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_link_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_pass_through_ext_link_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_link_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_pass_through_ext_link_optional(void *obj, H5VL_link_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);

/* Object callbacks */
static void *H5VL_pass_through_ext_object_open(void *obj, const H5VL_loc_params_t *loc_params, H5I_type_t *opened_type, hid_t dxpl_id, void **req);
static herr_t H5VL_pass_through_ext_object_copy(void *src_obj, const H5VL_loc_params_t *src_loc_params, const char *src_name, void *dst_obj, const H5VL_loc_params_t *dst_loc_params, const char *dst_name, hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_pass_through_ext_object_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_pass_through_ext_object_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_pass_through_ext_object_optional(void *obj, H5VL_object_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);

/* Container/connector introspection callbacks */
static herr_t H5VL_pass_through_ext_introspect_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl, const H5VL_class_t **conn_cls);
static herr_t H5VL_pass_through_ext_introspect_opt_query(void *obj, H5VL_subclass_t cls, int opt_type, hbool_t *supported);

/* Async request callbacks */
static herr_t H5VL_pass_through_ext_request_wait(void *req, uint64_t timeout, H5ES_status_t *status);
static herr_t H5VL_pass_through_ext_request_notify(void *obj, H5VL_request_notify_t cb, void *ctx);
static herr_t H5VL_pass_through_ext_request_cancel(void *req);
static herr_t H5VL_pass_through_ext_request_specific(void *req, H5VL_request_specific_t specific_type, va_list arguments);
static herr_t H5VL_pass_through_ext_request_optional(void *req, H5VL_request_optional_t opt_type, va_list arguments);
static herr_t H5VL_pass_through_ext_request_free(void *req);

/* Blob callbacks */
static herr_t H5VL_pass_through_ext_blob_put(void *obj, const void *buf, size_t size, void *blob_id, void *ctx);
static herr_t H5VL_pass_through_ext_blob_get(void *obj, const void *blob_id, void *buf, size_t size, void *ctx);
static herr_t H5VL_pass_through_ext_blob_specific(void *obj, void *blob_id, H5VL_blob_specific_t specific_type, va_list arguments);
static herr_t H5VL_pass_through_ext_blob_optional(void *obj, void *blob_id, H5VL_blob_optional_t opt_type, va_list arguments);

/* Token callbacks */
static herr_t H5VL_pass_through_ext_token_cmp(void *obj, const H5O_token_t *token1, const H5O_token_t *token2, int *cmp_value);
static herr_t H5VL_pass_through_ext_token_to_str(void *obj, H5I_type_t obj_type, const H5O_token_t *token, char **token_str);
static herr_t H5VL_pass_through_ext_token_from_str(void *obj, H5I_type_t obj_type, const char *token_str, H5O_token_t *token);

/* Generic optional callback */
static herr_t H5VL_pass_through_ext_optional(void *obj, int op_type, hid_t dxpl_id, void **req, va_list arguments);

herr_t H5VL_pass_through_ext_file_cache_create(void *obj, const char *name, hid_t fapl_id, 
					hsize_t size,
					cache_purpose_t purpose,
					cache_duration_t duration); 
static herr_t
file_get_wrapper(void *file, hid_t driver_id, H5VL_file_get_t get_type, hid_t dxpl_id,
		 void **req, ...);

/*******************/
/* Local variables */
/*******************/

/* Pass through VOL connector class struct */
static const H5VL_class_t H5VL_pass_through_ext_g = {
    H5VL_PASSTHRU_EXT_VERSION,                          /* version      */
    (H5VL_class_value_t)H5VL_PASSTHRU_EXT_VALUE,        /* value        */
    H5VL_PASSTHRU_EXT_NAME,                             /* name         */
    0,                                              /* capability flags */
    H5VL_pass_through_ext_init,                         /* initialize   */
    H5VL_pass_through_ext_term,                         /* terminate    */
    {                                           /* info_cls */
        sizeof(H5VL_pass_through_ext_info_t),           /* size    */
        H5VL_pass_through_ext_info_copy,                /* copy    */
        H5VL_pass_through_ext_info_cmp,                 /* compare */
        H5VL_pass_through_ext_info_free,                /* free    */
        H5VL_pass_through_ext_info_to_str,              /* to_str  */
        H5VL_pass_through_ext_str_to_info               /* from_str */
    },
    {                                           /* wrap_cls */
        H5VL_pass_through_ext_get_object,               /* get_object   */
        H5VL_pass_through_ext_get_wrap_ctx,             /* get_wrap_ctx */
        H5VL_pass_through_ext_wrap_object,              /* wrap_object  */
        H5VL_pass_through_ext_unwrap_object,            /* unwrap_object */
        H5VL_pass_through_ext_free_wrap_ctx             /* free_wrap_ctx */
    },
    {                                           /* attribute_cls */
        H5VL_pass_through_ext_attr_create,              /* create */
        H5VL_pass_through_ext_attr_open,                /* open */
        H5VL_pass_through_ext_attr_read,                /* read */
        H5VL_pass_through_ext_attr_write,               /* write */
        H5VL_pass_through_ext_attr_get,                 /* get */
        H5VL_pass_through_ext_attr_specific,            /* specific */
        H5VL_pass_through_ext_attr_optional,            /* optional */
        H5VL_pass_through_ext_attr_close                /* close */
    },
    {                                           /* dataset_cls */
        H5VL_pass_through_ext_dataset_create,           /* create */
        H5VL_pass_through_ext_dataset_open,             /* open */
        H5VL_pass_through_ext_dataset_read,             /* read */
        H5VL_pass_through_ext_dataset_write,            /* write */
        H5VL_pass_through_ext_dataset_get,              /* get */
        H5VL_pass_through_ext_dataset_specific,         /* specific */
        H5VL_pass_through_ext_dataset_optional,         /* optional */
        H5VL_pass_through_ext_dataset_close             /* close */
    },
    {                                           /* datatype_cls */
        H5VL_pass_through_ext_datatype_commit,          /* commit */
        H5VL_pass_through_ext_datatype_open,            /* open */
        H5VL_pass_through_ext_datatype_get,             /* get_size */
        H5VL_pass_through_ext_datatype_specific,        /* specific */
        H5VL_pass_through_ext_datatype_optional,        /* optional */
        H5VL_pass_through_ext_datatype_close            /* close */
    },
    {                                           /* file_cls */
        H5VL_pass_through_ext_file_create,              /* create */
        H5VL_pass_through_ext_file_open,                /* open */
        H5VL_pass_through_ext_file_get,                 /* get */
        H5VL_pass_through_ext_file_specific,            /* specific */
        H5VL_pass_through_ext_file_optional,            /* optional */
        H5VL_pass_through_ext_file_close                /* close */
    },
    {                                           /* group_cls */
        H5VL_pass_through_ext_group_create,             /* create */
        H5VL_pass_through_ext_group_open,               /* open */
        H5VL_pass_through_ext_group_get,                /* get */
        H5VL_pass_through_ext_group_specific,           /* specific */
        H5VL_pass_through_ext_group_optional,           /* optional */
        H5VL_pass_through_ext_group_close               /* close */
    },
    {                                           /* link_cls */
        H5VL_pass_through_ext_link_create,              /* create */
        H5VL_pass_through_ext_link_copy,                /* copy */
        H5VL_pass_through_ext_link_move,                /* move */
        H5VL_pass_through_ext_link_get,                 /* get */
        H5VL_pass_through_ext_link_specific,            /* specific */
        H5VL_pass_through_ext_link_optional             /* optional */
    },
    {                                           /* object_cls */
        H5VL_pass_through_ext_object_open,              /* open */
        H5VL_pass_through_ext_object_copy,              /* copy */
        H5VL_pass_through_ext_object_get,               /* get */
        H5VL_pass_through_ext_object_specific,          /* specific */
        H5VL_pass_through_ext_object_optional           /* optional */
    },
    {                                           /* introspect_cls */
        H5VL_pass_through_ext_introspect_get_conn_cls,  /* get_conn_cls */
        H5VL_pass_through_ext_introspect_opt_query,     /* opt_query */
    },
    {                                           /* request_cls */
        H5VL_pass_through_ext_request_wait,             /* wait */
        H5VL_pass_through_ext_request_notify,           /* notify */
        H5VL_pass_through_ext_request_cancel,           /* cancel */
        H5VL_pass_through_ext_request_specific,         /* specific */
        H5VL_pass_through_ext_request_optional,         /* optional */
        H5VL_pass_through_ext_request_free              /* free */
    },
    {                                           /* blob_cls */
        H5VL_pass_through_ext_blob_put,                 /* put */
        H5VL_pass_through_ext_blob_get,                 /* get */
        H5VL_pass_through_ext_blob_specific,            /* specific */
        H5VL_pass_through_ext_blob_optional             /* optional */
    },
    {                                           /* token_cls */
        H5VL_pass_through_ext_token_cmp,                /* cmp */
        H5VL_pass_through_ext_token_to_str,             /* to_str */
        H5VL_pass_through_ext_token_from_str              /* from_str */
    },
    H5VL_pass_through_ext_optional                  /* optional */
};

/* The connector identification number, initialized at runtime */
static hid_t H5VL_PASSTHRU_EXT_g = H5I_INVALID_HID;
/* Operation values for new "API" routines */
/* These are initialized in the VOL connector's 'init' callback at runtime.
 *      It's good practice to reset them back to -1 in the 'term' callback.
 */
static int H5VL_passthru_dataset_foo_op_g = -1;
static int H5VL_passthru_dataset_bar_op_g = -1;
static int H5VL_passthru_group_fiddle_op_g = -1;
static int H5VL_passthru_dataset_read_to_cache_op_g = -1;  
static int H5VL_passthru_dataset_read_from_cache_op_g = -1;
static int H5VL_passthru_dataset_mmap_remap_op_g = -1;

static int H5VL_passthru_dataset_cache_create_op_g = -1;
static int H5VL_passthru_dataset_cache_remove_op_g = -1; 

static int H5VL_passthru_file_cache_create_op_g = -1; // this is for reserving cache space for the file
static int H5VL_passthru_file_cache_remove_op_g = -1; //

/* Define Local storage property list */
hid_t H5P_CLS_LOCAL_STORAGE_CREATE_ID_g; 
/* Global Local Storage variable */
static LocalStorage H5LS; 
/* Required shim routines, to enable dynamic loading of shared library */
/* The HDF5 library _must_ find routines with these names and signatures
 *      for a shared library that contains a VOL connector to be detected
 *      and loaded at runtime.
 */
H5PL_type_t H5PLget_plugin_type(void) {return H5PL_TYPE_VOL;}
const void *H5PLget_plugin_info(void) {return &H5VL_pass_through_ext_g;}

void LOG(int rank, const char *str) {
  if (debug_level()>0) 
    printf("[%d] %s\n", rank, str);
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
    assert(-1 != H5VL_passthru_dataset_foo_op_g);

    /* Call the VOL dataset optional routine, requesting 'foo' occur */
    if(H5VLdataset_optional_op(dset_id, H5VL_passthru_dataset_foo_op_g, dxpl_id, req, i, d) < 0)
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
    assert(-1 != H5VL_passthru_dataset_read_to_cache_op_g);
    
    if(H5VLdataset_optional_op(dset_id, H5VL_passthru_dataset_read_to_cache_op_g, plist_id, req, 
			       mem_type_id, mem_space_id, 
			       file_space_id, buf) < 0) 
      return (-1);
    return 0; 
} /* end H5Dread_to_cache ()*/


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

  assert(-1 != H5VL_passthru_dataset_read_from_cache_op_g);
  if(H5VLdataset_optional_op(dset_id, H5VL_passthru_dataset_read_from_cache_op_g, plist_id, NULL, 
			     mem_type_id, mem_space_id, 
			     file_space_id, plist_id, buf) < 0) 
    return (-1);
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
H5Dmmap_remap(hid_t dset_id) {
  assert(-1 != H5VL_passthru_dataset_mmap_remap_op_g);
  if(H5VLdataset_optional_op(dset_id, H5VL_passthru_dataset_mmap_remap_op_g, H5P_DATASET_XFER_DEFAULT, NULL) < 0) 
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
  assert(-1 != H5VL_passthru_dataset_cache_remove_op_g);
  if(H5VLdataset_optional_op(dset_id, H5VL_passthru_dataset_cache_remove_op_g, H5P_DATASET_XFER_DEFAULT, NULL) < 0)
    return (-1);
  return 0; 
}

herr_t 
H5Dcache_create(hid_t dset_id, char *name) {
  assert(-1 != H5VL_passthru_dataset_cache_create_op_g);
  if(H5VLdataset_optional_op(dset_id, H5VL_passthru_dataset_cache_create_op_g, H5P_DATASET_XFER_DEFAULT, NULL, name) < 0)
    return (-1);
  return 0; 
}

herr_t
H5Fcache_create(hid_t file_id, hid_t dapl_id, hsize_t size, cache_purpose_t purpose, cache_duration_t duration) {
  /* Sanity check */
  assert(-1 !=H5VL_passthru_file_cache_create_op_g);
  /* Call the VOL file optional routine */
  if (H5VLfile_optional_op(file_id, H5VL_passthru_file_cache_create_op_g,
			   H5P_DATASET_XFER_DEFAULT, NULL,
			   dapl_id, size, purpose, duration) < 0)
    return (-1);
  return 0; 
}

herr_t
H5Fcache_remove(hid_t file_id) {
  /* Sanity check */
  assert(-1 !=H5VL_passthru_file_cache_remove_op_g);
  /* Call the VOL file optional routine */
  if (H5VLfile_optional_op(file_id, H5VL_passthru_file_cache_remove_op_g, H5P_DATASET_XFER_DEFAULT, NULL) < 0)
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
    assert(-1 != H5VL_passthru_dataset_bar_op_g);

    /* Call the VOL dataset optional routine, requesting 'bar' occur */
    if(H5VLdataset_optional_op(dset_id, H5VL_passthru_dataset_bar_op_g, dxpl_id, req, dp, up) < 0)
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
    assert(-1 != H5VL_passthru_group_fiddle_op_g);

    /* Call the VOL group optional routine, requesting 'fiddle' occur */
    if(H5VLgroup_optional_op(dset_id, H5VL_passthru_group_fiddle_op_g, dxpl_id, req) < 0)
        return(-1);

    return 0;
} /* end H5Gfiddle() */


/*-------------------------------------------------------------------------
 * Function:    H5VL__pass_through_new_obj
 *
 * Purpose:     Create a new pass through object for an underlying object
 *
 * Return:      Success:    Pointer to the new pass through object
 *              Failure:    NULL
 *
 * Programmer:  Quincey Koziol
 *              Monday, December 3, 2018
 *
 *-------------------------------------------------------------------------
 */
static H5VL_pass_through_ext_t *
H5VL_pass_through_ext_new_obj(void *under_obj, hid_t under_vol_id)
{
    H5VL_pass_through_ext_t *new_obj;

    new_obj = (H5VL_pass_through_ext_t *)calloc(1, sizeof(H5VL_pass_through_ext_t));
    new_obj->under_object = under_obj;
    new_obj->under_vol_id = under_vol_id;
    H5Iinc_ref(new_obj->under_vol_id);

    return new_obj;
} /* end H5VL__pass_through_new_obj() */


/*-------------------------------------------------------------------------
 * Function:    H5VL__pass_through_free_obj
 *
 * Purpose:     Release a pass through object
 *
 * Note:	Take care to preserve the current HDF5 error stack
 *		when calling HDF5 API calls.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 * Programmer:  Quincey Koziol
 *              Monday, December 3, 2018
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_free_obj(H5VL_pass_through_ext_t *obj)
{
    hid_t err_id;

    err_id = H5Eget_current_stack();

    H5Idec_ref(obj->under_vol_id);

    H5Eset_current_stack(err_id);

    free(obj);

    return 0;
} /* end H5VL__pass_through_free_obj() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_register
 *
 * Purpose:     Register the pass-through VOL connector and retrieve an ID
 *              for it.
 *
 * Return:      Success:    The ID for the pass-through VOL connector
 *              Failure:    -1
 *
 * Programmer:  Quincey Koziol
 *              Wednesday, November 28, 2018
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5VL_pass_through_ext_register(void)
{
    /* Singleton register the pass-through VOL connector ID */
    if(H5VL_PASSTHRU_EXT_g < 0)
        H5VL_PASSTHRU_EXT_g = H5VLregister_connector(&H5VL_pass_through_ext_g, H5P_DEFAULT);

    return H5VL_PASSTHRU_EXT_g;
} /* end H5VL_pass_through_ext_register() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_init
 *
 * Purpose:     Initialize this VOL connector, performing any necessary
 *              operations for the connector that will apply to all containers
 *              accessed with the connector.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_init(hid_t vipl_id)
{
  int rank;
  int provided;
  printf("%s:%d: Pass_through VOL is called.\n", __func__, __LINE__);
#ifdef ENABLE_EXT_PASSTHRU_LOGGING
  printf("------- EXT PASS THROUGH VOL INIT\n");
#endif

    /* Shut compiler up about unused parameter */
    vipl_id = vipl_id;

    /* Acquire operation values for new "API" routines to use */
    assert(-1 == H5VL_passthru_dataset_foo_op_g);
    if(H5VLregister_opt_operation(H5VL_SUBCLS_DATASET, &H5VL_passthru_dataset_foo_op_g) < 0)
      return(-1);
    
    assert(-1 == H5VL_passthru_dataset_read_to_cache_op_g);
    if(H5VLregister_opt_operation(H5VL_SUBCLS_DATASET, &H5VL_passthru_dataset_read_to_cache_op_g) < 0)
      return(-1);
    
    assert(-1 == H5VL_passthru_dataset_read_from_cache_op_g);
    if(H5VLregister_opt_operation(H5VL_SUBCLS_DATASET, &H5VL_passthru_dataset_read_from_cache_op_g) < 0)
      return(-1);

    assert(-1 == H5VL_passthru_dataset_cache_remove_op_g);
    if(H5VLregister_opt_operation(H5VL_SUBCLS_DATASET, &H5VL_passthru_dataset_cache_remove_op_g) < 0)
      return(-1);

    assert(-1 == H5VL_passthru_dataset_cache_create_op_g);
    if(H5VLregister_opt_operation(H5VL_SUBCLS_DATASET, &H5VL_passthru_dataset_cache_create_op_g) < 0)
      return(-1);

    assert(-1 == H5VL_passthru_dataset_mmap_remap_op_g);
    if(H5VLregister_opt_operation(H5VL_SUBCLS_DATASET, &H5VL_passthru_dataset_mmap_remap_op_g) < 0)
      return -1; 
    
    assert(-1 == H5VL_passthru_dataset_bar_op_g);
    if(H5VLregister_opt_operation(H5VL_SUBCLS_DATASET, &H5VL_passthru_dataset_bar_op_g) < 0)
        return(-1);
    
    assert(-1 == H5VL_passthru_group_fiddle_op_g);
    if(H5VLregister_opt_operation(H5VL_SUBCLS_DATASET, &H5VL_passthru_group_fiddle_op_g) < 0)
        return(-1);
    assert(-1 == H5VL_passthru_file_cache_remove_op_g);
    if(H5VLregister_opt_operation(H5VL_SUBCLS_FILE, &H5VL_passthru_file_cache_remove_op_g) < 0)
      return (-1);
    
    assert(-1 == H5VL_passthru_file_cache_create_op_g);
    if(H5VLregister_opt_operation(H5VL_SUBCLS_FILE, &H5VL_passthru_file_cache_create_op_g) < 0)
      return (-1);
    
    
    // setting global local storage properties
    char ls_path[255]="./";
    hsize_t ls_size=137438953472; 
    cache_storage_t ls_type=SSD;
    cache_replacement_policy_t ls_replacement = LRU; 
    
    // get the path 
    if (getenv("HDF5_LOCAL_STORAGE_PATH")) 
        strcpy(ls_path, getenv("HDF5_LOCAL_STORAGE_PATH")); 
    else 
        strcpy(ls_path, "./"); 
    
    // get the storage size
    if (getenv("HDF5_LOCAL_STORAGE_SIZE"))
      ls_size = atof(getenv("HDF5_LOCAL_STORAGE_SIZE")); 
    
    // get the storate type
    if (getenv("HDF5_LOCAL_STORAGE_TYPE")) {
      if (strcmp(getenv("HDF5_LOCAL_STORAGE_TYPE"), "SSD")==0) {
	ls_type = SSD;
      } else if (strcmp(getenv("HDF5_LOCAL_STORAGE_TYPE"), "BURST_BUFFER")==0) {
	ls_type = BURST_BUFFER; 
      }  else if (strcmp(getenv("HDF5_LOCAL_STORAGE_TYPE"), "MEMORY")==0) {
	ls_type = MEMORY;
      }
      if (debug_level()>0) printf("HDF5_LOCAL_STORAGE_TYPE: %s", getenv("HDF5_LOCAL_STORAGE_TYPE"));
    }
    HDF5_WRITE_CACHE_SIZE = 2147483648; //setting the default cache size to be 2 GB; 

    if (getenv("HDF5_WRITE_CACHE_SIZE")) {
      HDF5_WRITE_CACHE_SIZE = atof(getenv("HDF5_WRITE_CACHE_SIZE"));
    }
    if (getenv("HDF5_CACHE_REPLACEMENT")) {
      char *a=getenv("HDF5_CACHE_REPLACEMENT"); 
      if (strcmp(a, "LRU")) {
        ls_replacement = LRU; 
      } else if (strcmp(a, "LRU")) {
        ls_replacement = LRU; 
      } else if (strcmp(a, "FIFO")) {
        ls_replacement = FIFO; 
      } else {
        printf("Unknown cache replacement policy. Setting it to be LRU\n"); 
      }
    }

    H5P_CLS_LOCAL_STORAGE_CREATE_ID_g = H5Pcreate_class(H5P_ROOT, "H5P_LOCAL_STORAGE_CREATE", NULL, NULL, NULL, NULL, NULL, NULL);
    herr_t ret = H5Pregister2(H5P_CLS_LOCAL_STORAGE_CREATE_ID_g, "TYPE", sizeof(cache_storage_t), &ls_type, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    ret = H5Pregister2(H5P_CLS_LOCAL_STORAGE_CREATE_ID_g, "PATH", sizeof(ls_path), &ls_path, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    ret = H5Pregister2(H5P_CLS_LOCAL_STORAGE_CREATE_ID_g, "SIZE", sizeof(ls_size), &ls_size, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    ret = H5Pregister2(H5P_CLS_LOCAL_STORAGE_CREATE_ID_g, "REPLACEMENT_POLICY", sizeof(ls_replacement), &ls_replacement, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    //H5LSset(&H5LS, ls_type, ls_path, ls_size, ls_replacement); 
    hid_t plist = H5Pcreate(H5P_LOCAL_STORAGE_CREATE);
    H5LS = *H5LScreate(plist);
    H5Pclose(plist);
    return 0;
} /* end H5VL_pass_through_ext_init() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_term
 *
 * Purpose:     Terminate this VOL connector, performing any necessary
 *              operations for the connector that release connector-wide
 *              resources (usually created / initialized with the 'init'
 *              callback).
 *
 * Return:      Success:    0
 *              Failure:    (Can't fail)
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_term(void)
{
#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL TERM\n");
#endif
    
    /* Reset VOL ID */
    H5VL_PASSTHRU_EXT_g = H5I_INVALID_HID;
    
    /* Reset operation values for new "API" routines */
    assert(-1 != H5VL_passthru_dataset_foo_op_g);
    H5VL_passthru_dataset_foo_op_g = (-1);
    
    assert(-1 != H5VL_passthru_dataset_read_to_cache_op_g);
    H5VL_passthru_dataset_read_to_cache_op_g = (-1);
    
    assert(-1 != H5VL_passthru_dataset_read_from_cache_op_g);
    H5VL_passthru_dataset_read_from_cache_op_g = (-1);

    assert(-1 != H5VL_passthru_dataset_cache_remove_op_g);
    H5VL_passthru_dataset_cache_remove_op_g = (-1);

    assert(-1 != H5VL_passthru_dataset_cache_create_op_g);
    H5VL_passthru_dataset_cache_create_op_g = (-1);

    assert(-1 != H5VL_passthru_dataset_mmap_remap_op_g); 
    H5VL_passthru_dataset_mmap_remap_op_g = (-1);
    
    assert(-1 != H5VL_passthru_dataset_bar_op_g);
    H5VL_passthru_dataset_bar_op_g = (-1);
    
    assert(-1 != H5VL_passthru_group_fiddle_op_g);
    H5VL_passthru_group_fiddle_op_g = (-1);
    
    assert(-1 != H5VL_passthru_file_cache_create_op_g);
    H5VL_passthru_file_cache_create_op_g = (-1);

    //H5Pclose_class(H5P_CLS_LOCAL_STORAGE_ID_g);
    return 0;
} /* end H5VL_pass_through_ext_term() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_info_copy
 *
 * Purpose:     Duplicate the connector's info object.
 *
 * Returns:     Success:    New connector info object
 *              Failure:    NULL
 *
 *---------------------------------------------------------------------------
 */
static void *
H5VL_pass_through_ext_info_copy(const void *_info)
{
    const H5VL_pass_through_ext_info_t *info = (const H5VL_pass_through_ext_info_t *)_info;
    H5VL_pass_through_ext_info_t *new_info;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL INFO Copy\n");
#endif

    /* Allocate new VOL info struct for the pass through connector */
    new_info = (H5VL_pass_through_ext_info_t *)calloc(1, sizeof(H5VL_pass_through_ext_info_t));

    /* Increment reference count on underlying VOL ID, and copy the VOL info */
    new_info->under_vol_id = info->under_vol_id;
    H5Iinc_ref(new_info->under_vol_id);
    if(info->under_vol_info)
        H5VLcopy_connector_info(new_info->under_vol_id, &(new_info->under_vol_info), info->under_vol_info);

    return new_info;
} /* end H5VL_pass_through_ext_info_copy() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_info_cmp
 *
 * Purpose:     Compare two of the connector's info objects, setting *cmp_value,
 *              following the same rules as strcmp().
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_info_cmp(int *cmp_value, const void *_info1, const void *_info2)
{
    const H5VL_pass_through_ext_info_t *info1 = (const H5VL_pass_through_ext_info_t *)_info1;
    const H5VL_pass_through_ext_info_t *info2 = (const H5VL_pass_through_ext_info_t *)_info2;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL INFO Compare\n");
#endif

    /* Sanity checks */
    assert(info1);
    assert(info2);

    /* Initialize comparison value */
    *cmp_value = 0;

    /* Compare under VOL connector classes */
    H5VLcmp_connector_cls(cmp_value, info1->under_vol_id, info2->under_vol_id);
    if(*cmp_value != 0)
        return 0;

    /* Compare under VOL connector info objects */
    H5VLcmp_connector_info(cmp_value, info1->under_vol_id, info1->under_vol_info, info2->under_vol_info);
    if(*cmp_value != 0)
        return 0;

    return 0;
} /* end H5VL_pass_through_ext_info_cmp() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_info_free
 *
 * Purpose:     Release an info object for the connector.
 *
 * Note:	Take care to preserve the current HDF5 error stack
 *		when calling HDF5 API calls.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_info_free(void *_info)
{
    H5VL_pass_through_ext_info_t *info = (H5VL_pass_through_ext_info_t *)_info;
    hid_t err_id;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL INFO Free\n");
#endif

    err_id = H5Eget_current_stack();

    /* Release underlying VOL ID and info */
    if(info->under_vol_info)
        H5VLfree_connector_info(info->under_vol_id, info->under_vol_info);
    H5Idec_ref(info->under_vol_id);

    H5Eset_current_stack(err_id);

    /* Free pass through info object itself */
    free(info);

    return 0;
} /* end H5VL_pass_through_ext_info_free() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_info_to_str
 *
 * Purpose:     Serialize an info object for this connector into a string
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_info_to_str(const void *_info, char **str)
{
    const H5VL_pass_through_ext_info_t *info = (const H5VL_pass_through_ext_info_t *)_info;
    H5VL_class_value_t under_value = (H5VL_class_value_t)-1;
    char *under_vol_string = NULL;
    size_t under_vol_str_len = 0;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL INFO To String\n");
#endif

    /* Get value and string for underlying VOL connector */
    H5VLget_value(info->under_vol_id, &under_value);
    H5VLconnector_info_to_str(info->under_vol_info, info->under_vol_id, &under_vol_string);

    /* Determine length of underlying VOL info string */
    if(under_vol_string)
        under_vol_str_len = strlen(under_vol_string);

    /* Allocate space for our info */
    *str = (char *)H5allocate_memory(32 + under_vol_str_len, (hbool_t)0);
    assert(*str);

    /* Encode our info
     * Normally we'd use snprintf() here for a little extra safety, but that
     * call had problems on Windows until recently. So, to be as platform-independent
     * as we can, we're using sprintf() instead.
     */
    sprintf(*str, "under_vol=%u;under_info={%s}", (unsigned)under_value, (under_vol_string ? under_vol_string : ""));

    return 0;
} /* end H5VL_pass_through_ext_info_to_str() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_str_to_info
 *
 * Purpose:     Deserialize a string into an info object for this connector.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_str_to_info(const char *str, void **_info)
{
    H5VL_pass_through_ext_info_t *info;
    unsigned under_vol_value;
    const char *under_vol_info_start, *under_vol_info_end;
    hid_t under_vol_id;
    void *under_vol_info = NULL;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL INFO String To Info\n");
#endif

    /* Retrieve the underlying VOL connector value and info */
    sscanf(str, "under_vol=%u;", &under_vol_value);
    under_vol_id = H5VLregister_connector_by_value((H5VL_class_value_t)under_vol_value, H5P_DEFAULT);
    under_vol_info_start = strchr(str, '{');
    under_vol_info_end = strrchr(str, '}');
    assert(under_vol_info_end > under_vol_info_start);
    if(under_vol_info_end != (under_vol_info_start + 1)) {
      char *under_vol_info_str;
      
      under_vol_info_str = (char *)malloc((size_t)(under_vol_info_end - under_vol_info_start));
      memcpy(under_vol_info_str, under_vol_info_start + 1, (size_t)((under_vol_info_end - under_vol_info_start) - 1));
      *(under_vol_info_str + (under_vol_info_end - under_vol_info_start)) = '\0';
      
      H5VLconnector_str_to_info(under_vol_info_str, under_vol_id, &under_vol_info);
      
      free(under_vol_info_str);
    } /* end else */

    /* Allocate new pass-through VOL connector info and set its fields */
    info = (H5VL_pass_through_ext_info_t *)calloc(1, sizeof(H5VL_pass_through_ext_info_t));
    info->under_vol_id = under_vol_id;
    info->under_vol_info = under_vol_info;

    /* Set return value */
    *_info = info;

    return 0;
} /* end H5VL_pass_through_ext_str_to_info() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_get_object
 *
 * Purpose:     Retrieve the 'data' for a VOL object.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static void *
H5VL_pass_through_ext_get_object(const void *obj)
{
    const H5VL_pass_through_ext_t *o = (const H5VL_pass_through_ext_t *)obj;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL Get object\n");
#endif

    return H5VLget_object(o->under_object, o->under_vol_id);
} /* end H5VL_pass_through_ext_get_object() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_get_wrap_ctx
 *
 * Purpose:     Retrieve a "wrapper context" for an object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_get_wrap_ctx(const void *obj, void **wrap_ctx)
{
    const H5VL_pass_through_ext_t *o = (const H5VL_pass_through_ext_t *)obj;
    H5VL_pass_through_ext_wrap_ctx_t *new_wrap_ctx;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL WRAP CTX Get\n");
#endif

    /* Allocate new VOL object wrapping context for the pass through connector */
    new_wrap_ctx = (H5VL_pass_through_ext_wrap_ctx_t *)calloc(1, sizeof(H5VL_pass_through_ext_wrap_ctx_t));

    /* Increment reference count on underlying VOL ID, and copy the VOL info */
    new_wrap_ctx->under_vol_id = o->under_vol_id;
    H5Iinc_ref(new_wrap_ctx->under_vol_id);
    H5VLget_wrap_ctx(o->under_object, o->under_vol_id, &new_wrap_ctx->under_wrap_ctx);

    /* Set wrap context to return */
    *wrap_ctx = new_wrap_ctx;

    return 0;
} /* end H5VL_pass_through_ext_get_wrap_ctx() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_wrap_object
 *
 * Purpose:     Use a "wrapper context" to wrap a data object
 *
 * Return:      Success:    Pointer to wrapped object
 *              Failure:    NULL
 *
 *---------------------------------------------------------------------------
 */
static void *
H5VL_pass_through_ext_wrap_object(void *obj, H5I_type_t obj_type, void *_wrap_ctx)
{
    H5VL_pass_through_ext_wrap_ctx_t *wrap_ctx = (H5VL_pass_through_ext_wrap_ctx_t *)_wrap_ctx;
    H5VL_pass_through_ext_t *new_obj;
    void *under;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL WRAP Object\n");
#endif

    /* Wrap the object with the underlying VOL */
    under = H5VLwrap_object(obj, obj_type, wrap_ctx->under_vol_id, wrap_ctx->under_wrap_ctx);
    if(under)
        new_obj = H5VL_pass_through_ext_new_obj(under, wrap_ctx->under_vol_id);
    else
        new_obj = NULL;

    return new_obj;
} /* end H5VL_pass_through_ext_wrap_object() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_unwrap_object
 *
 * Purpose:     Unwrap a wrapped object, discarding the wrapper, but returning
 *		underlying object.
 *
 * Return:      Success:    Pointer to unwrapped object
 *              Failure:    NULL
 *
 *---------------------------------------------------------------------------
 */
static void *
H5VL_pass_through_ext_unwrap_object(void *obj)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    void *under;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL UNWRAP Object\n");
#endif

    /* Unrap the object with the underlying VOL */
    under = H5VLunwrap_object(o->under_object, o->under_vol_id);

    if(under)
        H5VL_pass_through_ext_free_obj(o);

    return under;
} /* end H5VL_pass_through_ext_unwrap_object() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_free_wrap_ctx
 *
 * Purpose:     Release a "wrapper context" for an object
 *
 * Note:	Take care to preserve the current HDF5 error stack
 *		when calling HDF5 API calls.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_free_wrap_ctx(void *_wrap_ctx)
{
    H5VL_pass_through_ext_wrap_ctx_t *wrap_ctx = (H5VL_pass_through_ext_wrap_ctx_t *)_wrap_ctx;
    hid_t err_id;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL WRAP CTX Free\n");
#endif

    err_id = H5Eget_current_stack();

    /* Release underlying VOL ID and wrap context */
    if(wrap_ctx->under_wrap_ctx)
        H5VLfree_wrap_ctx(wrap_ctx->under_wrap_ctx, wrap_ctx->under_vol_id);
    H5Idec_ref(wrap_ctx->under_vol_id);

    H5Eset_current_stack(err_id);

    /* Free pass through wrap context object itself */
    free(wrap_ctx);

    return 0;
} /* end H5VL_pass_through_ext_free_wrap_ctx() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_attr_create
 *
 * Purpose:     Creates an attribute on an object.
 *
 * Return:      Success:    Pointer to attribute object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_pass_through_ext_attr_create(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id,
    hid_t aapl_id, hid_t dxpl_id, void **req)
{
    H5VL_pass_through_ext_t *attr;
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    void *under;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL ATTRIBUTE Create\n");
#endif

    under = H5VLattr_create(o->under_object, loc_params, o->under_vol_id, name, type_id, space_id, acpl_id, aapl_id, dxpl_id, req);
    if(under) {
        attr = H5VL_pass_through_ext_new_obj(under, o->under_vol_id);

        /* Check for async request */
        if(req && *req)
            *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);
    } /* end if */
    else
        attr = NULL;

    return (void*)attr;
} /* end H5VL_pass_through_ext_attr_create() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_attr_open
 *
 * Purpose:     Opens an attribute on an object.
 *
 * Return:      Success:    Pointer to attribute object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_pass_through_ext_attr_open(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t aapl_id, hid_t dxpl_id, void **req)
{
    H5VL_pass_through_ext_t *attr;
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    void *under;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL ATTRIBUTE Open\n");
#endif

    under = H5VLattr_open(o->under_object, loc_params, o->under_vol_id, name, aapl_id, dxpl_id, req);
    if(under) {
        attr = H5VL_pass_through_ext_new_obj(under, o->under_vol_id);

        /* Check for async request */
        if(req && *req)
            *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);
    } /* end if */
    else
        attr = NULL;

    return (void *)attr;
} /* end H5VL_pass_through_ext_attr_open() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_attr_read
 *
 * Purpose:     Reads data from attribute.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_attr_read(void *attr, hid_t mem_type_id, void *buf,
    hid_t dxpl_id, void **req)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)attr;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL ATTRIBUTE Read\n");
#endif

    ret_value = H5VLattr_read(o->under_object, o->under_vol_id, mem_type_id, buf, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_attr_read() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_attr_write
 *
 * Purpose:     Writes data to attribute.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_attr_write(void *attr, hid_t mem_type_id, const void *buf,
    hid_t dxpl_id, void **req)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)attr;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL ATTRIBUTE Write\n");
#endif

    ret_value = H5VLattr_write(o->under_object, o->under_vol_id, mem_type_id, buf, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_attr_write() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_attr_get
 *
 * Purpose:     Gets information about an attribute
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_attr_get(void *obj, H5VL_attr_get_t get_type, hid_t dxpl_id,
    void **req, va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL ATTRIBUTE Get\n");
#endif

    ret_value = H5VLattr_get(o->under_object, o->under_vol_id, get_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_attr_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_attr_specific
 *
 * Purpose:     Specific operation on attribute
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_attr_specific(void *obj, const H5VL_loc_params_t *loc_params,
    H5VL_attr_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL ATTRIBUTE Specific\n");
#endif

    ret_value = H5VLattr_specific(o->under_object, loc_params, o->under_vol_id, specific_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_attr_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_attr_optional
 *
 * Purpose:     Perform a connector-specific operation on an attribute
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_attr_optional(void *obj, H5VL_attr_optional_t opt_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL ATTRIBUTE Optional\n");
#endif

    ret_value = H5VLattr_optional(o->under_object, o->under_vol_id, opt_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_attr_optional() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_attr_close
 *
 * Purpose:     Closes an attribute.
 *
 * Return:      Success:    0
 *              Failure:    -1, attr not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_attr_close(void *attr, hid_t dxpl_id, void **req)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)attr;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL ATTRIBUTE Close\n");
#endif

    ret_value = H5VLattr_close(o->under_object, o->under_vol_id, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);

    /* Release our wrapper, if underlying attribute was closed */
    if(ret_value >= 0)
        H5VL_pass_through_ext_free_obj(o);

    return ret_value;
} /* end H5VL_pass_through_ext_attr_close() */

static herr_t
dataset_get_wrapper(void *dset, hid_t driver_id, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, ...)
{
    herr_t ret;
    va_list args;
    va_start(args, req);
    ret = H5VLdataset_get(dset, driver_id, get_type, dxpl_id, req, args);
    va_end(args);
    return ret;
}



/*-------------------------------------------------------------------------
 * Function:    H5Dcreate_mmap_win
 *
 * Purpose:     create memory map files on the local storage and attached it to a MPI window for read cache.
 *
 * Return:      Success:    0
 *              Failure:    -1
 * Comment:    
 *-------------------------------------------------------------------------
 */
static herr_t
H5Dcreate_mmap_win(void *obj, const char *prefix) {
  H5VL_pass_through_ext_t *dset = (H5VL_pass_through_ext_t*) obj; 
  // created a memory mapped file on the local storage. And create a MPI_win 
  hsize_t ss = (dset->H5DRMM->dset.size/PAGESIZE+1)*PAGESIZE;
  if (dset->H5DRMM->H5LS->storage!=MEMORY) {
    strcpy(dset->H5DRMM->mmap.fname, dset->H5DRMM->cache->path);
    strcat(dset->H5DRMM->mmap.fname, prefix); 
    strcat(dset->H5DRMM->mmap.fname, "-");
    char cc[255];
    int2char(dset->H5DRMM->mpi.rank, cc);
    strcat(dset->H5DRMM->mmap.fname, cc);
    strcat(dset->H5DRMM->mmap.fname, ".dat");
    int fh = open(dset->H5DRMM->mmap.fname, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    char a = 'A';
    pwrite(fh, &a, 1, ss);
    fsync(fh);
    close(fh);
    dset->H5DRMM->mmap.fd = open(dset->H5DRMM->mmap.fname, O_RDWR);
    dset->H5DRMM->mmap.buf = mmap(NULL, ss, PROT_READ | PROT_WRITE, MAP_SHARED, dset->H5DRMM->mmap.fd, 0);
    //msync(dset->H5DRMM->mmap.buf, ss, MS_SYNC);
  } else {
    dset->H5DRMM->mmap.buf = malloc(ss); 
  }
  // create a new MPI data type based on the size of the element. 
  MPI_Datatype type[1] = {MPI_BYTE};
  int blocklen[1] = {dset->H5DRMM->dset.esize};
  MPI_Aint disp[1] = {0};
  MPI_Type_create_struct(1, blocklen, disp, type, &dset->H5DRMM->dset.mpi_datatype);
  MPI_Type_commit(&dset->H5DRMM->dset.mpi_datatype);
  
  // creeate MPI windows for both main threead and I/O thread. 
  MPI_Win_create(dset->H5DRMM->mmap.buf, ss, dset->H5DRMM->dset.esize, MPI_INFO_NULL, dset->H5DRMM->mpi.comm, &dset->H5DRMM->mpi.win);
  MPI_Comm_dup(dset->H5DRMM->mpi.comm, &dset->H5DRMM->mpi.comm_t);
  MPI_Win_create(dset->H5DRMM->mmap.buf, ss, dset->H5DRMM->dset.esize, MPI_INFO_NULL, dset->H5DRMM->mpi.comm_t, &dset->H5DRMM->mpi.win_t);
  LOG(dset->H5DRMM->mpi.rank, "Created MMAP");
  return SUCCEED; 
}



/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_dataset_read_cache_create
 *
 * Purpose:     creating dataset cache for read purpose 
 *
 * Return:      Success:    0
 *              Failure:    -1
 * Comment:   
 *-------------------------------------------------------------------------
 */
static herr_t 
H5VL_pass_through_ext_dataset_read_cache_create(void *obj, const char *name)
{
  // set up read cache: obj, dset object
    // loc - where is the dataset located - group or file object
  herr_t ret_value; 
  H5VL_pass_through_ext_t *dset = (H5VL_pass_through_ext_t *) obj;
  H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)dset->parent; 
  while (o->parent!=NULL)
    o = (H5VL_pass_through_ext_t*) o->parent; 

  if (dset->H5DRMM==NULL) {
    dset->H5DRMM = (H5Dread_cache_metadata *) malloc(sizeof(H5Dread_cache_metadata));
  } else {
    printf("Already set"); 
    return SUCCEED; 
  }
  dset->read_cache = true; 
  hsize_t size_f;
  char fname[255];
  file_get_wrapper(dset->under_object, dset->under_vol_id, H5VL_FILE_GET_NAME, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL, (int)H5I_DATASET, size_f, fname, &ret_value);
  void **req;
  if (o->H5DRMM==NULL) {
    o->read_cache = true;
    hid_t fapl_id;
    file_get_wrapper(o->under_object, o->under_vol_id, H5VL_FILE_GET_FAPL, H5P_DATASET_XFER_DEFAULT, req, &fapl_id);
    fapl_id = fapl_id - 5; // I'm not sure why I have to manually substract 5;
    H5VL_pass_through_ext_file_cache_create((void *)o, fname, fapl_id, 0, READ, TEMPORAL);
  }
  LOG(o->H5DRMM->mpi.rank, "read_cache_create");
  dset->H5DRMM->H5LS = o->H5DRMM->H5LS;
  MPI_Comm_dup(o->H5DRMM->mpi.comm, &dset->H5DRMM->mpi.comm);
  dset->H5DRMM->mpi.rank = o->H5DRMM->mpi.rank;
  dset->H5DRMM->mpi.nproc = o->H5DRMM->mpi.nproc;
  dset->H5DRMM->mpi.ppn = o->H5DRMM->mpi.ppn;
  dset->H5DRMM->mpi.local_rank = o->H5DRMM->mpi.local_rank;
  int np; 
  MPI_Comm_rank(dset->H5DRMM->mpi.comm, &np);
  printf("np, rank: %d %d\n", np, dset->H5DRMM->mpi.rank);
  pthread_cond_init(&dset->H5DRMM->io.io_cond, NULL);
  pthread_cond_init(&dset->H5DRMM->io.master_cond, NULL);
  pthread_mutex_init(&dset->H5DRMM->io.request_lock, NULL);
  dset->H5DRMM->io.batch_cached = true;
  dset->H5DRMM->io.dset_cached = false;
  srand(time(NULL));   // Initialization, should only be called once.
  hid_t unused; 
  dataset_get_wrapper(dset->under_object, dset->under_vol_id, H5VL_DATASET_GET_TYPE, unused, req, &dset->H5DRMM->dset.h5_datatype);
  dset->H5DRMM->dset.esize = H5Tget_size(dset->H5DRMM->dset.h5_datatype);
  hid_t fspace;
  dataset_get_wrapper(dset->under_object, dset->under_vol_id, H5VL_DATASET_GET_SPACE, unused, req, &fspace);
  int ndims = H5Sget_simple_extent_ndims(fspace);
  hsize_t *gdims = (hsize_t*) malloc(ndims*sizeof(hsize_t));
  H5Sget_simple_extent_dims(fspace, gdims, NULL);
  hsize_t dim = 1; // compute the size of a single sample
  for(int i=1; i<ndims; i++) {
    dim = dim*gdims[i];
  }
  dset->H5DRMM->dset.sample.nel = dim;
  dset->H5DRMM->dset.sample.dim = ndims-1;
  dset->H5DRMM->dset.ns_glob = gdims[0];
  dset->H5DRMM->dset.ns_cached = 0;
  parallel_dist(gdims[0], dset->H5DRMM->mpi.nproc, dset->H5DRMM->mpi.rank, &dset->H5DRMM->dset.ns_loc, &dset->H5DRMM->dset.s_offset);
  dset->H5DRMM->dset.sample.size = dset->H5DRMM->dset.esize*dset->H5DRMM->dset.sample.nel;
  dset->H5DRMM->dset.size = dset->H5DRMM->dset.sample.size*dset->H5DRMM->dset.ns_loc;
  LOG(dset->H5DRMM->mpi.rank, "Claim space");
  if (H5LSclaim_space(dset->H5DRMM->H5LS, dset->H5DRMM->dset.size, HARD, dset->H5DRMM->H5LS->replacement_policy) == SUCCEED) { 
    dset->H5DRMM->cache = (LocalStorageCache*) malloc(sizeof(LocalStorageCache));
    dset->H5DRMM->cache->mspace_total = dset->H5DRMM->dset.size; 
    dset->H5DRMM->cache->mspace_left = dset->H5DRMM->cache->mspace_total;
    dset->H5DRMM->cache->mspace_per_rank_total = dset->H5DRMM->cache->mspace_total / dset->H5DRMM->mpi.ppn;
    dset->H5DRMM->cache->mspace_per_rank_left = dset->H5DRMM->cache->mspace_per_rank_total;
    strcpy(dset->H5DRMM->cache->path, o->H5DRMM->cache->path); // create 
    strcat(dset->H5DRMM->cache->path, "/");
    strcat(dset->H5DRMM->cache->path, name);
    strcat(dset->H5DRMM->cache->path, "-cache/");
    mkdir(dset->H5DRMM->cache->path, 0755); // setup the folder with the name of the file, and put everything under it.
    H5LSregister_cache(dset->H5DRMM->H5LS, dset->H5DRMM->cache, (void *) dset);
    H5Dcreate_mmap_win((void*)dset, name);
    int rc = pthread_create(&dset->H5DRMM->io.pthread, NULL, H5Dread_pthread_func_vol, dset->H5DRMM);
    free(gdims);
    dset->read_cache_info_set = true;
    return SUCCEED;
  } else {
    if (dset->H5DRMM->mpi.rank==0) 
      printf("Unable to allocate space to the dataset for cache; read cache function will be turned off\n");
    dset->read_cache = false;
    free(dset->H5DRMM);
    dset->H5DRMM=NULL; 
    return FAIL; 
  }
}



/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_dataset_create
 *
 * Purpose:     Creates a dataset in a container
 *
 * Return:      Success:    Pointer to a dataset object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_pass_through_ext_dataset_create(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id,
    hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    H5VL_pass_through_ext_t *dset;
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    void *under;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL DATASET Create\n");
#endif

    under = H5VLdataset_create(o->under_object, loc_params, o->under_vol_id, name, lcpl_id, type_id, space_id, dcpl_id,  dapl_id, dxpl_id, req);
    if(under) {
        dset = H5VL_pass_through_ext_new_obj(under, o->under_vol_id);
	dset->parent = obj; 
        /* Check for async request */
        if(req && *req)
            *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);
    } /* end if */
    else
        dset = NULL;

    /* inherit cache information from loc */
    if (o->write_cache) { 
      dset->H5DWMM = o->H5DWMM;
      dset->write_cache = o->write_cache;
      dset->num_request_dataset = 0;
    }
    if (o->read_cache) {
      dset->H5DRMM = o->H5DRMM; 
      dset->read_cache = o->read_cache; 
      H5VL_pass_through_ext_dataset_read_cache_create(dset, name);
    }

    return (void *)dset;
} /* end H5VL_pass_through_ext_dataset_create() */



static herr_t
H5VL_pass_through_ext_dataset_mmap_remap(void *obj) {
  H5VL_pass_through_ext_t *dset = (H5VL_pass_through_ext_t*) obj; 
  // created a memory mapped file on the local storage. And create a MPI_win 
  hsize_t ss = (dset->H5DRMM->dset.size/PAGESIZE+1)*PAGESIZE;
  if (dset->H5DRMM->H5LS->storage!=MEMORY) {
    pthread_mutex_lock(&dset->H5DRMM->io.request_lock);
    while(!dset->H5DRMM->io.batch_cached) {
      pthread_cond_signal(&dset->H5DRMM->io.io_cond);
      pthread_cond_wait(&dset->H5DRMM->io.master_cond, &dset->H5DRMM->io.request_lock);
    }
    pthread_mutex_unlock(&dset->H5DRMM->io.request_lock);
    munmap(dset->H5DRMM->mmap.buf, ss);
    MPI_Win_free(&dset->H5DRMM->mpi.win);
    MPI_Win_free(&dset->H5DRMM->mpi.win_t);
    close(dset->H5DRMM->mmap.fd);
    dset->H5DRMM->mmap.fd = open(dset->H5DRMM->mmap.fname, O_RDWR);
    dset->H5DRMM->mmap.buf = mmap(NULL, ss, PROT_READ | PROT_WRITE, MAP_SHARED, dset->H5DRMM->mmap.fd, 0);
    //msync(dset->H5DRMM->mmap.buf, ss, MS_ASYNC);
    MPI_Win_create(dset->H5DRMM->mmap.buf, ss, dset->H5DRMM->dset.esize, MPI_INFO_NULL, dset->H5DRMM->mpi.comm, &dset->H5DRMM->mpi.win);
    MPI_Win_create(dset->H5DRMM->mmap.buf, ss, dset->H5DRMM->dset.esize, MPI_INFO_NULL, dset->H5DRMM->mpi.comm_t, &dset->H5DRMM->mpi.win_t);
    LOG(dset->H5DRMM->mpi.rank, "Remap MMAP");
  } 
  return SUCCEED; 
}

/* This is to wrap the dataset get function*/

/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_dataset_open
 *
 * Purpose:     Opens a dataset in a container
 *
 * Return:      Success:    Pointer to a dataset object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_pass_through_ext_dataset_open(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    H5VL_pass_through_ext_t *dset;
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    void *under;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL DATASET Open\n");
#endif

    under = H5VLdataset_open(o->under_object, loc_params, o->under_vol_id, name, dapl_id, dxpl_id, req);
    if(under) {
      dset = H5VL_pass_through_ext_new_obj(under, o->under_vol_id);
      dset->read_cache = o->read_cache;
      dset->write_cache = o->write_cache; 
      dset->parent = obj;
      dset->H5DRMM=NULL; 
      /* setup read cache */
      if (dset->read_cache && (dset->H5DRMM==NULL)) {
	H5VL_pass_through_ext_dataset_read_cache_create(dset, name);
      }
      /* Check for async request */
      if(req && *req)
	*req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);
    } /* end if */
    else
      dset = NULL;

    return (void *)dset;
} /* end H5VL_pass_through_ext_dataset_open() */


/*-------------------------------------------------------------------------
 * Function:    H5Dread_pthread_func_vol
 *
 * Purpose:     Pthread function for storing read dataset to the local storage
 *
 * Return:      NULL
 *
 *-------------------------------------------------------------------------
 */
void *H5Dread_pthread_func_vol(void *args) {
  H5Dread_cache_metadata *dmm = (H5Dread_cache_metadata*) args;
  pthread_mutex_lock(&dmm->io.request_lock);
  while (!dmm->io.dset_cached) {
    if (!dmm->io.batch_cached) {
      char *p_mem = (char *) dmm->mmap.tmp_buf;
      MPI_Win_fence(MPI_MODE_NOPRECEDE, dmm->mpi.win_t);
      int batch_size = dmm->dset.batch.size;
      if (dmm->dset.contig_read) {
	int dest = dmm->dset.batch.list[0];
	int src = dest/dmm->dset.ns_loc;
	assert(src < dmm->mpi.nproc);
	MPI_Aint offset = (dest%dmm->dset.ns_loc)*dmm->dset.sample.nel;
	MPI_Put(p_mem, dmm->dset.sample.nel*batch_size,
		dmm->dset.mpi_datatype, src,
		offset, dmm->dset.sample.nel*batch_size,
		dmm->dset.mpi_datatype, dmm->mpi.win_t);
      } else {
	for(int i=0; i<batch_size; i++) {
	  int dest = dmm->dset.batch.list[i];
	  int src = dest/dmm->dset.ns_loc;
	  assert(src < dmm->mpi.nproc);
	  MPI_Aint offset = (dest%dmm->dset.ns_loc)*dmm->dset.sample.nel;
	  MPI_Put(&p_mem[i*dmm->dset.sample.size],
		  dmm->dset.sample.nel,
		  dmm->dset.mpi_datatype, src,
		  offset, dmm->dset.sample.nel,
		  dmm->dset.mpi_datatype, dmm->mpi.win_t);
	}
      }
      MPI_Win_fence(MPI_MODE_NOSUCCEED, dmm->mpi.win_t);
      H5LSrecord_cache_access(dmm->cache);
      dmm->io.batch_cached = true;
      dmm->dset.ns_cached += dmm->dset.batch.size;
      bool dset_cached; 
      if (dmm->dset.ns_cached>=dmm->dset.ns_loc) {
	dmm->io.dset_cached=true;
      }
      MPI_Allreduce(&dmm->io.dset_cached, &dset_cached, 1, MPI_C_BOOL, MPI_LAND, dmm->mpi.comm_t);
      dmm->io.dset_cached = dset_cached; 
    } else {
      pthread_cond_signal(&dmm->io.master_cond);
      pthread_cond_wait(&dmm->io.io_cond, &dmm->io.request_lock); 
    }
  }
  pthread_mutex_unlock(&dmm->io.request_lock);
  return NULL;
}



/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_dataset_read_to_cache
 *
 * Purpose:     Reads data elements from a dataset into a buffer and stores
 *              a copy to the local storage
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_dataset_read_to_cache(void *dset, hid_t mem_type_id, hid_t mem_space_id,
    hid_t file_space_id, hid_t plist_id, void *buf, void **req)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)dset;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL DATASET Read to cache\n");
#endif

    ret_value = H5VLdataset_read(o->under_object, o->under_vol_id, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
    /* Saving the read buffer to local storage */
    if (o->read_cache) {
      LOG(o->H5DRMM->mpi.rank, "dataset_read_to_cache");
      hsize_t bytes = get_buf_size(mem_space_id, mem_type_id);
      get_samples_from_filespace(file_space_id, &o->H5DRMM->dset.batch, &o->H5DRMM->dset.contig_read);
      if (o->H5DRMM->mmap.tmp_buf != NULL) free(o->H5DRMM->mmap.tmp_buf);
      o->H5DRMM->mmap.tmp_buf = malloc(bytes);
      memcpy(o->H5DRMM->mmap.tmp_buf, buf, bytes);
      /* Waking up the I/O thread */
      if (debug_level()>1 && o->H5DRMM->mpi.rank==io_node())printf("waking up to the I/O thread\n");
      pthread_mutex_lock(&o->H5DRMM->io.request_lock);
      o->H5DRMM->io.batch_cached = false;
      pthread_cond_signal(&o->H5DRMM->io.io_cond);
      pthread_mutex_unlock(&o->H5DRMM->io.request_lock);
    } 
    if(req && *req) 
      *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);
    return ret_value;
} /* end H5VL_pass_through_ext_dataset_read_to_cache() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_dataset_read_from_cache
 *
 * Purpose:     Reads data elements from a dataset cache into a buffer.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_dataset_read_from_cache(void *dset, hid_t mem_type_id, hid_t mem_space_id,
    hid_t file_space_id, hid_t plist_id, void *buf, void **req)
{
  H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)dset;
  herr_t ret_value;
  
#ifdef ENABLE_EXT_PASSTHRU_LOGGING
  printf("------- EXT PASS THROUGH VOL DATASET Read\n");
#endif
  if (o->read_cache) {
    bool contig = false;
    BATCH b;
    LOG(o->H5DRMM->mpi.rank, "dataset_read_from_cache");
    get_samples_from_filespace(file_space_id, &b, &contig);
    MPI_Win_fence(MPI_MODE_NOPUT | MPI_MODE_NOPRECEDE, o->H5DRMM->mpi.win);
    char *p_mem = (char *) buf;
    int batch_size = b.size;
    if (!contig) {
      for(int i=0; i< batch_size; i++) {
	int dest = b.list[i];
	int src = dest/o->H5DRMM->dset.ns_loc;
	MPI_Aint offset = (dest%o->H5DRMM->dset.ns_loc)*o->H5DRMM->dset.sample.nel;
	MPI_Get(&p_mem[i*o->H5DRMM->dset.sample.size],
		o->H5DRMM->dset.sample.nel,
		o->H5DRMM->dset.mpi_datatype, src,
		offset, o->H5DRMM->dset.sample.nel,
		o->H5DRMM->dset.mpi_datatype, o->H5DRMM->mpi.win);
      }
    } else {
      int dest = b.list[0];
      int src = dest/o->H5DRMM->dset.ns_loc;
      MPI_Aint offset = (dest%o->H5DRMM->dset.ns_loc)*o->H5DRMM->dset.sample.nel;
      MPI_Get(p_mem, o->H5DRMM->dset.sample.nel*batch_size,
	      o->H5DRMM->dset.mpi_datatype, src,
	      offset, o->H5DRMM->dset.sample.nel*batch_size,
	      o->H5DRMM->dset.mpi_datatype, o->H5DRMM->mpi.win);
    }
    MPI_Win_fence(MPI_MODE_NOSUCCEED, o->H5DRMM->mpi.win);
    H5LSrecord_cache_access(o->H5DRMM->cache);
    ret_value = 0; 
  } else {
    ret_value = H5VLdataset_read(o->under_object, o->under_vol_id, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
  }
  if(req && *req)
    *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);
  return ret_value;
} /* end H5VL_pass_through_ext_dataset_read_from_cache() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_dataset_read
 *
 * Purpose:     Reads data elements from a dataset into a buffer.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id,
    hid_t file_space_id, hid_t plist_id, void *buf, void **req)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)dset;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL DATASET Read\n");
#endif
    if (o->read_cache) {
      pthread_mutex_lock(&o->H5DRMM->io.request_lock);
      while(!o->H5DRMM->io.batch_cached) {
	pthread_cond_signal(&o->H5DRMM->io.io_cond);
	pthread_cond_wait(&o->H5DRMM->io.master_cond, &o->H5DRMM->io.request_lock);
      }
      pthread_mutex_unlock(&o->H5DRMM->io.request_lock);
      if (debug_level()>0) printf("[%d] o->H5DRMM: %d (cached) %zu (total) %d (total_cached?)\n", o->H5DRMM->mpi.rank, o->H5DRMM->dset.ns_cached, o->H5DRMM->dset.ns_loc, o->H5DRMM->io.dset_cached);
      if (!o->H5DRMM->io.dset_cached)
	return H5VL_pass_through_ext_dataset_read_to_cache(dset, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
      else
	return H5VL_pass_through_ext_dataset_read_from_cache(dset, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
    }
    else {
      ret_value = H5VLdataset_read(o->under_object, o->under_vol_id, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
    }

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_dataset_read() */



/*-------------------------------------------------------------------------
 * Function:    H5Dwrite_pthread_func_vol
 *
 * Purpose:     Pthread function for migrating data from local storage to 
 *              the parallel file system
 *
 * Return:      NULL
 *
 *-------------------------------------------------------------------------
 */
void *H5Dwrite_pthread_func_vol(void *arg) {
  // this is to us the H5DWMM as an input
  H5Dwrite_cache_metadata *wmm = (H5Dwrite_cache_metadata*) arg;
  pthread_mutex_lock(&wmm->io.request_lock);
  bool loop = (wmm->io.num_request>=0);
  bool empty = (wmm->io.num_request==0);
  pthread_mutex_unlock(&wmm->io.request_lock);  
  while (loop) {
    if (!empty) {
      thread_data_t *task = wmm->io.current_request;
      if (wmm->mpi.rank== io_node() && debug_level()>0) {
	printf("\n===================================\n");
	printf("pthread: Executing I/O task %d\n", task->id);
      }
      if (wmm->H5LS->storage!=MEMORY) {
	task->buf = mmap(NULL, task->size, PROT_READ, MAP_SHARED, wmm->mmap.fd, task->offset);
	msync(task->buf, task->size, MS_SYNC);
      } else {
	char *p = (char *) wmm->mmap.buf; 
	task->buf = &p[task->offset];
      }
      H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)task->dataset_obj;
      hbool_t acquired=false;
      while(!acquired)
	H5TSmutex_acquire(&acquired);
      if (wmm->mpi.rank== io_node() && debug_level()>0) printf("pthread: acquired global mutex\n");
      H5VLrestore_lib_state(task->h5_state);
      void **req;
      pthread_mutex_lock(&wmm->io.request_lock);
      wmm->io.offset_current = task->offset; 
      pthread_mutex_unlock(&wmm->io.request_lock);  
#ifdef THETA
      wmm->mmap.tmp_buf = malloc(task->size);
      memcpy(wmm->mmap.tmp_buf, task->buf, task->size);
      H5VLdataset_write(o->under_object, o->under_vol_id,
			task->mem_type_id, task->mem_space_id,
			task->file_space_id, task->xfer_plist_id,
			wmm->mmap.tmp_buf, req);
      free(wmm->mmap.tmp_buf);
#else
      H5VLdataset_write(o->under_object, o->under_vol_id,
			task->mem_type_id, task->mem_space_id,
			task->file_space_id, task->xfer_plist_id,
			task->buf, req);
#endif
      H5LSrecord_cache_access(wmm->cache);
      if (wmm->mpi.rank==io_node() && debug_level()>0) printf("pthread: I/O task %d is done!\n", task->id);
      if (wmm->H5LS->storage !=MEMORY)
	munmap(task->buf, task->size);

      H5Sclose(task->mem_space_id);
      H5Sclose(task->file_space_id);
      H5Pclose(task->xfer_plist_id);
      H5Tclose(task->mem_type_id);
      H5VLreset_lib_state();
      H5VLfree_lib_state(task->h5_state);
      H5TSmutex_release();
      pthread_mutex_lock(&o->H5DWMM->io.request_lock);
      wmm->cache->mspace_per_rank_left = wmm->cache->mspace_per_rank_left + (task->size/PAGESIZE+1)*PAGESIZE;
      pthread_mutex_unlock(&o->H5DWMM->io.request_lock);
      if (wmm->mpi.rank==io_node() && debug_level()>0) printf("pthread: global mutex_released\n");
      if (wmm->mpi.rank== io_node() && debug_level()>0) {
	printf("===================================\n");
      }
      pthread_mutex_lock(&wmm->io.request_lock);
      wmm->io.current_request=wmm->io.current_request->next;
      wmm->io.num_request--;
      o->num_request_dataset--;
      pthread_mutex_unlock(&wmm->io.request_lock);
    }
    pthread_mutex_lock(&wmm->io.request_lock);
    loop = (wmm->io.num_request>=0);
    empty = (wmm->io.num_request==0);
    pthread_mutex_unlock(&wmm->io.request_lock);  
    if (empty) {
      pthread_mutex_lock(&wmm->io.request_lock);
      pthread_cond_signal(&wmm->io.master_cond);
      pthread_cond_wait(&wmm->io.io_cond, &wmm->io.request_lock);
      pthread_mutex_unlock(&wmm->io.request_lock);  
    }
    pthread_mutex_lock(&wmm->io.request_lock);
    loop = (wmm->io.num_request>=0);
    empty = (wmm->io.num_request==0);
    pthread_mutex_unlock(&wmm->io.request_lock);  
  }
  return NULL; 
} /* end H5Dwrite_pthread_func_vol */




/*-------------------------------------------------------------------------
 * Function:    H5Ssel_gather_write
 *
 * Purpose:     Copy the data buffer into local storage. 
 *
 * Return:      NULL
 *
 *-------------------------------------------------------------------------
 */
herr_t H5Ssel_gather_write(hid_t space, hid_t tid, const void *buf, int fd, hsize_t offset) {
  unsigned flags = H5S_SEL_ITER_GET_SEQ_LIST_SORTED;
  size_t elmt_size =  H5Tget_size(tid);
  hid_t iter = H5Ssel_iter_create(space, elmt_size, flags);
  size_t maxseq = H5Sget_select_npoints(space);
  size_t maxbytes = maxseq*elmt_size;
  size_t nseq, nbytes;
  size_t *len = (size_t*)malloc(maxseq*sizeof(size_t));
  hsize_t *off = (hsize_t*)malloc(maxseq*sizeof(hsize_t));
  H5Ssel_iter_get_seq_list(iter, maxseq, maxbytes, &nseq, &nbytes, off, len);
  hsize_t off_contig=0;
  char *p = (char*) buf; 
  for(int i=0; i<nseq; i++) {
    int err = pwrite(fd, &p[off[i]], len[i], offset+off_contig);
    off_contig += len[i];
  }
  fsync(fd);
  return 0;
}


/*-------------------------------------------------------------------------
 * Function:    H5Ssel_gather_copy
 *
 * Purpose:     Copy the data buffer into memory. 
 *
 * Return:      NULL
 *
 *-------------------------------------------------------------------------
 */
herr_t H5Ssel_gather_copy(hid_t space, hid_t tid, const void *buf, void *mbuf, hsize_t offset) {
  unsigned flags = H5S_SEL_ITER_GET_SEQ_LIST_SORTED;
  size_t elmt_size =  H5Tget_size(tid);
  hid_t iter = H5Ssel_iter_create(space, elmt_size, flags);
  size_t maxseq = H5Sget_select_npoints(space);
  size_t maxbytes = maxseq*elmt_size;
  size_t nseq, nbytes;
  size_t *len = (size_t*)malloc(maxseq*sizeof(size_t));
  hsize_t *off = (hsize_t*)malloc(maxseq*sizeof(hsize_t));
  H5Ssel_iter_get_seq_list(iter, maxseq, maxbytes, &nseq, &nbytes, off, len);
  hsize_t off_contig=0;
  char *p = (char*) buf;
  char *mp = (char*) mbuf; 
  for(int i=0; i<nseq; i++) {
    memcpy(&mp[offset+off_contig], &p[off[i]], len[i]); 
    off_contig += len[i];
  }
  return 0;
} /* end  H5Ssel_gather_copy() */



/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_dataset_write
 *
 * Purpose:     Writes data elements from a buffer into a dataset.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */

static herr_t
H5VL_pass_through_ext_dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id,
    hid_t file_space_id, hid_t plist_id, const void *buf, void **req)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)dset;
    herr_t ret_value;
#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL DATASET Write\n");
#endif
    if (o->write_cache) {
      hsize_t size = get_buf_size(mem_space_id, mem_type_id);
      H5TSmutex_release();
      if (io_node()==o->H5DWMM->mpi.rank && debug_level()>0) printf("main thread: release mutex\n"); 
      pthread_mutex_lock(&o->H5DWMM->io.request_lock);
      //
      if (o->H5DWMM->io.offset_current < o->H5DWMM->mmap.offset) {
	if (o->H5DWMM->mmap.offset + size > o->H5DWMM->cache->mspace_per_rank_total) {
	  while (o->H5DWMM->io.offset_current < size) {
	    pthread_cond_signal(&o->H5DWMM->io.io_cond);
	    pthread_cond_wait(&o->H5DWMM->io.master_cond, &o->H5DWMM->io.request_lock);
	  }
	  o->H5DWMM->mmap.offset = 0;
	}
      } else if (o->H5DWMM->io.offset_current > o->H5DWMM->mmap.offset) {
	while (o->H5DWMM->io.offset_current < o->H5DWMM->mmap.offset + size) {
	  pthread_cond_signal(&o->H5DWMM->io.io_cond);
	  pthread_cond_wait(&o->H5DWMM->io.master_cond, &o->H5DWMM->io.request_lock);
	}
      }
      pthread_mutex_unlock(&o->H5DWMM->io.request_lock);
      if (o->H5DWMM->H5LS->storage !=  MEMORY)
	H5Ssel_gather_write(mem_space_id, mem_type_id, buf, o->H5DWMM->mmap.fd, o->H5DWMM->mmap.offset);
      else
	H5Ssel_gather_copy(mem_space_id, mem_type_id, buf, o->H5DWMM->mmap.buf, o->H5DWMM->mmap.offset);
      o->H5DWMM->io.request_list->offset = o->H5DWMM->mmap.offset; 
      o->H5DWMM->mmap.offset += (size/PAGESIZE+1)*PAGESIZE;
      pthread_mutex_lock(&o->H5DWMM->io.request_lock);
      o->H5DWMM->cache->mspace_per_rank_left = o->H5DWMM->cache->mspace_per_rank_left - (size/PAGESIZE+1)*PAGESIZE;
      pthread_mutex_unlock(&o->H5DWMM->io.request_lock);
      if (o->H5DWMM->H5LS->storage!=MEMORY) {
#ifdef __APPLE__
	fcntl(o->H5DWMM->mmap.fd, F_NOCACHE, 1);
#else
	fsync(o->H5DWMM->mmap.fd);
#endif
      }
      o->H5DWMM->io.request_list->dataset_obj = dset; 
      // retrieve current library state;
      hbool_t acq=false;
      while(!acq)
	H5TSmutex_acquire(&acq);
      if (io_node()==o->H5DWMM->mpi.rank && debug_level()>0) printf("main thread: acquire mutex\n"); 
      H5VLretrieve_lib_state(&o->H5DWMM->io.request_list->h5_state);
      o->H5DWMM->io.request_list->mem_type_id = H5Tcopy(mem_type_id);
      hsize_t ldims[1] = {H5Sget_select_npoints(mem_space_id)};
      o->H5DWMM->io.request_list->mem_space_id = H5Screate_simple(1, ldims, NULL);
      o->H5DWMM->io.request_list->file_space_id = H5Scopy(file_space_id);
      o->H5DWMM->io.request_list->xfer_plist_id = H5Pcopy(plist_id);
      o->H5DWMM->io.request_list->size = size; 
      o->H5DWMM->io.request_list->next = (thread_data_t*) malloc(sizeof(thread_data_t));
      if (o->H5DWMM->mpi.rank==io_node() && debug_level()>0) printf("added task %d to the list;\n", o->H5DWMM->io.request_list->id);
      o->H5DWMM->io.request_list->next->id = o->H5DWMM->io.request_list->id + 1;
      o->H5DWMM->io.request_list = o->H5DWMM->io.request_list->next;
      
      // waken up the Background thread
      //      H5TSmutex_release();
      //      if (io_node()==o->H5DWMM->mpi.rank && debug_level()>0) printf("main thread: release mutex\n"); 
      //acq = false;
      pthread_mutex_lock(&o->H5DWMM->io.request_lock);
      o->H5DWMM->io.num_request++;
      o->num_request_dataset++;
      pthread_cond_signal(&o->H5DWMM->io.io_cond);// wake up I/O thread rightawayx
      pthread_mutex_unlock(&o->H5DWMM->io.request_lock);
      //
      ret_value=SUCCEED;
      //while(!acq)
      //H5TSmutex_acquire(&acq);
      //      if (io_node()==o->H5DWMM->mpi.rank && debug_level()>0) printf("main thread: acquire mutex\n"); 
    } else {
      ret_value = H5VLdataset_write(o->under_object, o->under_vol_id, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
      if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);
    }
    /* Check for async request */
    return ret_value;
} /* end H5VL_pass_through_ext_dataset_write() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_dataset_get
 *
 * Purpose:     Gets information about a dataset
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_dataset_get(void *dset, H5VL_dataset_get_t get_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)dset;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL DATASET Get\n");
#endif

    ret_value = H5VLdataset_get(o->under_object, o->under_vol_id, get_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_dataset_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_dataset_specific
 *
 * Purpose:     Specific operation on a dataset
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_dataset_specific(void *obj, H5VL_dataset_specific_t specific_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    hid_t under_vol_id;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL H5Dspecific\n");
#endif

    // Save copy of underlying VOL connector ID and prov helper, in case of
    // refresh destroying the current object
    under_vol_id = o->under_vol_id;

    ret_value = H5VLdataset_specific(o->under_object, o->under_vol_id, specific_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_dataset_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_dataset_cache_remove
 *
 * Purpose:     Closes a dataset.
 *
 * Return:      Success:    0
 *              Failure:    -1, dataset not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_dataset_cache_remove(void *dset, hid_t dxpl_id, void **req)
{
#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL DATASET Cache remove\n");
#endif
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)dset;
    herr_t ret_value = SUCCEED;
    if (o->write_cache) {
      H5TSmutex_release();
      pthread_mutex_lock(&o->H5DWMM->io.request_lock);
      while(o->num_request_dataset>0) {
	pthread_cond_signal(&o->H5DWMM->io.io_cond);
	pthread_cond_wait(&o->H5DWMM->io.master_cond, &o->H5DWMM->io.request_lock);
      }
      pthread_mutex_unlock(&o->H5DWMM->io.request_lock);
      hbool_t acq=false; 
      while(!acq)
	H5TSmutex_acquire(&acq);
      o->write_cache=false;
      free(o->H5DWMM);
      o->H5DWMM=NULL; 
    }
    if (o->read_cache) {
      H5TSmutex_release();
      pthread_mutex_lock(&o->H5DRMM->io.request_lock);
      while(!o->H5DRMM->io.batch_cached) {
	pthread_cond_signal(&o->H5DRMM->io.io_cond);
	pthread_cond_wait(&o->H5DRMM->io.master_cond, &o->H5DRMM->io.request_lock);
      }
      pthread_mutex_unlock(&o->H5DRMM->io.request_lock);
      pthread_mutex_lock(&o->H5DRMM->io.request_lock);
      o->H5DRMM->io.batch_cached=true;
      o->H5DRMM->io.dset_cached=true;
      pthread_cond_signal(&o->H5DRMM->io.io_cond);
      pthread_mutex_unlock(&o->H5DRMM->io.request_lock);
      pthread_join(o->H5DRMM->io.pthread, NULL);
      hbool_t acq=false; 
      while(!acq)
	H5TSmutex_acquire(&acq);
      MPI_Win_free(&o->H5DRMM->mpi.win);
      MPI_Win_free(&o->H5DRMM->mpi.win_t);
      MPI_Barrier(o->H5DRMM->mpi.comm);
      printf("%d, FREED MPIWIN\n", o->H5DRMM->mpi.rank);
      hsize_t ss = (o->H5DRMM->dset.size/PAGESIZE+1)*PAGESIZE;
      
      if (o->H5DRMM->H5LS->storage!=MEMORY) {
        munmap(o->H5DRMM->mmap.buf, ss);
        free(o->H5DRMM->mmap.tmp_buf);
        close(o->H5DRMM->mmap.fd);
      } else {
        free(o->H5DRMM->mmap.buf);
      }
      if (H5LSremove_cache(o->H5DRMM->H5LS, o->H5DRMM->cache)!=SUCCEED) {
	printf("UNABLE TO REMOVE CACHE: %s\n", o->H5DRMM->cache->path); 
      }
      o->read_cache = false;
      o->read_cache_info_set = false;
      printf("%d, FREED\n", o->H5DRMM->mpi.rank);
      MPI_Barrier(o->H5DRMM->mpi.comm);
      free(o->H5DRMM);
      o->H5DRMM=NULL;
    }
    return ret_value;
} /* end H5VL_pass_through_ext_dataset_cache_remove() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_dataset_optional
 *
 * Purpose:     Perform a connector-specific operation on a dataset
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_dataset_optional(void *obj, H5VL_dataset_optional_t opt_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL DATASET Optional\n");
#endif
    /* Sanity check */
    assert(-1 != H5VL_passthru_dataset_foo_op_g);
    assert(-1 != H5VL_passthru_dataset_bar_op_g);
    assert(-1 != H5VL_passthru_dataset_read_to_cache_op_g);
    assert(-1 != H5VL_passthru_dataset_read_from_cache_op_g);
    assert(-1 != H5VL_passthru_dataset_mmap_remap_op_g);
    assert(-1 != H5VL_passthru_dataset_cache_create_op_g);
    assert(-1 != H5VL_passthru_dataset_cache_remove_op_g);

    /* Capture and perform connector-specific 'foo' and 'bar' operations */
    if(opt_type == H5VL_passthru_dataset_foo_op_g) {
        int i;
        double d;

        /* Retrieve varargs parameters for 'foo' operation */
        i = va_arg(arguments, int);
        d = va_arg(arguments, double);
	printf("foo: i = %d, d = %f\n", i, d);

        /* <do 'foo'> */

        /* Set return value */
        ret_value = 0;

    } else if(opt_type == H5VL_passthru_dataset_read_to_cache_op_g) {
      hid_t mem_type_id = va_arg(arguments, long int);
      hid_t mem_space_id = va_arg(arguments, long int);
      hid_t file_space_id = va_arg(arguments, long int);
      void *buf = va_arg(arguments, void *);
      // make sure that the data is cached before read
      if (o->read_cache) {
	pthread_mutex_lock(&o->H5DRMM->io.request_lock);
	while(!o->H5DRMM->io.batch_cached) {
	  pthread_cond_signal(&o->H5DRMM->io.io_cond);
	  pthread_cond_wait(&o->H5DRMM->io.master_cond, &o->H5DRMM->io.request_lock);
	}
	pthread_mutex_unlock(&o->H5DRMM->io.request_lock);
      }
      ret_value = H5VL_pass_through_ext_dataset_read_to_cache(obj, mem_type_id, mem_space_id, file_space_id, dxpl_id, buf, req);
      
    } else if(opt_type == H5VL_passthru_dataset_read_from_cache_op_g) {
      hid_t mem_type_id = va_arg(arguments, long int);
        hid_t mem_space_id = va_arg(arguments, long int);
        hid_t file_space_id = va_arg(arguments, long int);
        hid_t plist_id = va_arg(arguments, long int);
        void *buf = va_arg(arguments, void *);
	if (o->read_cache) {
	  pthread_mutex_lock(&o->H5DRMM->io.request_lock);
	  while(!o->H5DRMM->io.batch_cached) {
            pthread_cond_signal(&o->H5DRMM->io.io_cond);
            pthread_cond_wait(&o->H5DRMM->io.master_cond, 
			      &o->H5DRMM->io.request_lock);
	  }
	  pthread_mutex_unlock(&o->H5DRMM->io.request_lock);
	}
        ret_value = H5VL_pass_through_ext_dataset_read_from_cache(obj, mem_type_id, mem_space_id, file_space_id, dxpl_id, buf, req);
    } else if (opt_type == H5VL_passthru_dataset_mmap_remap_op_g) {
      ret_value = H5VL_pass_through_ext_dataset_mmap_remap(obj);
    } else if (opt_type == H5VL_passthru_dataset_cache_remove_op_g) {
      ret_value = H5VL_pass_through_ext_dataset_cache_remove(obj, dxpl_id, req);
    } else if (opt_type == H5VL_passthru_dataset_cache_create_op_g) {
      char *name = va_arg(arguments, char *);
      ret_value = H5VL_pass_through_ext_dataset_read_cache_create(obj, name); 
    } else if(opt_type == H5VL_passthru_dataset_bar_op_g) {
      double *dp;
      unsigned *up;

        /* Retrieve varargs parameters for 'bar' operation */
        dp = va_arg(arguments, double *);
        up = va_arg(arguments, unsigned *);
	printf("bar: dp = %p, up = %p\n", dp, up);

        /* <do 'bar'> */

        /* Set values to return to application in parameters */
        if(dp)
            *dp = 3.14159;
        if(up)
            *up = 42;

        /* Set return value */
        ret_value = 0;

    } else
      ret_value = H5VLdataset_optional(o->under_object, o->under_vol_id, opt_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_dataset_optional() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_dataset_close
 *
 * Purpose:     Closes a dataset.
 *
 * Return:      Success:    0
 *              Failure:    -1, dataset not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_dataset_close(void *dset, hid_t dxpl_id, void **req)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)dset;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL DATASET Close\n");
#endif
    if (o->write_cache) {
      H5TSmutex_release();
      pthread_mutex_lock(&o->H5DWMM->io.request_lock);
      while(o->num_request_dataset>0) {
	pthread_cond_signal(&o->H5DWMM->io.io_cond);
	pthread_cond_wait(&o->H5DWMM->io.master_cond, &o->H5DWMM->io.request_lock);
      }
      pthread_mutex_unlock(&o->H5DWMM->io.request_lock);
      hbool_t acq=false; 
      while(!acq)
	H5TSmutex_acquire(&acq);
    }
    if (o->read_cache) {
      printf("enter into here........\n"); 
      H5TSmutex_release();
      pthread_mutex_lock(&o->H5DRMM->io.request_lock);
      while(!o->H5DRMM->io.batch_cached) {
	pthread_cond_signal(&o->H5DRMM->io.io_cond);
	pthread_cond_wait(&o->H5DRMM->io.master_cond, &o->H5DRMM->io.request_lock);
      }
      pthread_mutex_unlock(&o->H5DRMM->io.request_lock);
      pthread_mutex_lock(&o->H5DRMM->io.request_lock);
      o->H5DRMM->io.batch_cached=true;
      o->H5DRMM->io.dset_cached=true;
      pthread_cond_signal(&o->H5DRMM->io.io_cond);
      pthread_mutex_unlock(&o->H5DRMM->io.request_lock);
      pthread_join(o->H5DRMM->io.pthread, NULL);
      hbool_t acq=false; 
      while(!acq)
	H5TSmutex_acquire(&acq);

      MPI_Win_free(&o->H5DRMM->mpi.win);
      MPI_Win_free(&o->H5DRMM->mpi.win_t);
      hsize_t ss = (o->H5DRMM->dset.size/PAGESIZE+1)*PAGESIZE;
      if (o->H5DRMM->H5LS->storage!=MEMORY) {
        munmap(o->H5DRMM->mmap.buf, ss);
        free(o->H5DRMM->mmap.tmp_buf);
        close(o->H5DRMM->mmap.fd);
      } else {
        free(o->H5DRMM->mmap.buf);
      }
      if (H5LSremove_cache(o->H5DRMM->H5LS, o->H5DRMM->cache)!=SUCCEED) {
	printf("UNABLE TO REMOVE CACHE: %s\n", o->H5DRMM->cache->path); 
      }
    }

    ret_value = H5VLdataset_close(o->under_object, o->under_vol_id, dxpl_id, req);
    
    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);

    /* Release our wrapper, if underlying dataset was closed */
    if(ret_value >= 0) 
      H5VL_pass_through_ext_free_obj(o);
    return ret_value;
} /* end H5VL_pass_through_ext_dataset_close() */





/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_datatype_commit
 *
 * Purpose:     Commits a datatype inside a container.
 *
 * Return:      Success:    Pointer to datatype object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_pass_through_ext_datatype_commit(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t type_id, hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id,
    hid_t dxpl_id, void **req)
{
    H5VL_pass_through_ext_t *dt;
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    void *under;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL DATATYPE Commit\n");
#endif

    under = H5VLdatatype_commit(o->under_object, loc_params, o->under_vol_id, name, type_id, lcpl_id, tcpl_id, tapl_id, dxpl_id, req);
    if(under) {
        dt = H5VL_pass_through_ext_new_obj(under, o->under_vol_id);

        /* Check for async request */
        if(req && *req)
            *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);
    } /* end if */
    else
        dt = NULL;

    return (void *)dt;
} /* end H5VL_pass_through_ext_datatype_commit() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_datatype_open
 *
 * Purpose:     Opens a named datatype inside a container.
 *
 * Return:      Success:    Pointer to datatype object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_pass_through_ext_datatype_open(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t tapl_id, hid_t dxpl_id, void **req)
{
    H5VL_pass_through_ext_t *dt;
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    void *under;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL DATATYPE Open\n");
#endif

    under = H5VLdatatype_open(o->under_object, loc_params, o->under_vol_id, name, tapl_id, dxpl_id, req);
    if(under) {
        dt = H5VL_pass_through_ext_new_obj(under, o->under_vol_id);

        /* Check for async request */
        if(req && *req)
            *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);
    } /* end if */
    else
        dt = NULL;

    return (void *)dt;
} /* end H5VL_pass_through_ext_datatype_open() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_datatype_get
 *
 * Purpose:     Get information about a datatype
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_datatype_get(void *dt, H5VL_datatype_get_t get_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)dt;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL DATATYPE Get\n");
#endif

    ret_value = H5VLdatatype_get(o->under_object, o->under_vol_id, get_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_datatype_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_datatype_specific
 *
 * Purpose:     Specific operations for datatypes
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_datatype_specific(void *obj, H5VL_datatype_specific_t specific_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    hid_t under_vol_id;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL DATATYPE Specific\n");
#endif

    // Save copy of underlying VOL connector ID and prov helper, in case of
    // refresh destroying the current object
    under_vol_id = o->under_vol_id;

    ret_value = H5VLdatatype_specific(o->under_object, o->under_vol_id, specific_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_datatype_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_datatype_optional
 *
 * Purpose:     Perform a connector-specific operation on a datatype
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_datatype_optional(void *obj, H5VL_datatype_optional_t opt_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL DATATYPE Optional\n");
#endif

    ret_value = H5VLdatatype_optional(o->under_object, o->under_vol_id, opt_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_datatype_optional() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_datatype_close
 *
 * Purpose:     Closes a datatype.
 *
 * Return:      Success:    0
 *              Failure:    -1, datatype not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_datatype_close(void *dt, hid_t dxpl_id, void **req)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)dt;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL DATATYPE Close\n");
#endif

    assert(o->under_object);

    ret_value = H5VLdatatype_close(o->under_object, o->under_vol_id, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);

    /* Release our wrapper, if underlying datatype was closed */
    if(ret_value >= 0)
        H5VL_pass_through_ext_free_obj(o);

    return ret_value;
} /* end H5VL_pass_through_ext_datatype_close() */

#include <libgen.h>
#include <string.h>

char *get_fname(const char *path) {
  char tmp[255];
  strcpy(tmp, path); 
  return basename(tmp);
}

static herr_t
file_get_wrapper(void *file, hid_t driver_id, H5VL_file_get_t get_type, hid_t dxpl_id,
		 void **req, ...)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *) file;
    herr_t ret;
    va_list args;
    va_start(args, req);
    ret = H5VLfile_get(file, driver_id, get_type, dxpl_id, req, args);
    va_end(args);
    return ret;
}


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_file_cache_create
 *
 * Purpose:     create a file cache on the local storage 
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t 
H5VL_pass_through_ext_file_cache_create(void *obj, const char *name, hid_t fapl_id, 
					hsize_t size,
					cache_purpose_t purpose,
					cache_duration_t duration) {
  herr_t ret_value;
  hsize_t size_f;
  H5VL_pass_through_ext_t *file = (H5VL_pass_through_ext_t *) obj;
  if (purpose == WRITE) {
    file->write_cache = true; 
    srand(time(NULL));   // Initialization, should only be called once.
    if (file->H5DWMM==NULL) file->H5DWMM = (H5Dwrite_cache_metadata*) malloc(sizeof(H5Dwrite_cache_metadata)); 
    else 
    {
      if (file->H5DWMM->mpi.rank == io_node()) printf("file_cache_create: cache data already exist. Remove first!\n");
      return SUCCEED; 
    }
      // this is to
    file->H5DWMM->H5LS = (LocalStorage *) malloc(sizeof(LocalStorage)); 
    if (H5Pget_fapl_cache(fapl_id, "LOCAL_STORAGE", file->H5DWMM->H5LS)<0) {
      free(file->H5DWMM->H5LS);
      file->H5DWMM->H5LS = &H5LS;
    }
    if (H5LSclaim_space(file->H5DWMM->H5LS, size, HARD, file->H5DWMM->H5LS->replacement_policy) == FAIL) {
      file->write_cache = false;
      return FAIL; 
    }
    MPI_Comm comm, comm_dup;
    MPI_Info mpi_info;
    H5Pget_fapl_mpio(fapl_id, &comm, &mpi_info);
    MPI_Comm_dup(comm, &file->H5DWMM->mpi.comm);
    MPI_Comm_rank(comm, &file->H5DWMM->mpi.rank);
    MPI_Comm_size(comm, &file->H5DWMM->mpi.nproc);
    MPI_Comm_split_type(file->H5DWMM->mpi.comm, MPI_COMM_TYPE_SHARED, 0, MPI_INFO_NULL, &file->H5DWMM->mpi.node_comm);
    MPI_Comm_rank(file->H5DWMM->mpi.node_comm, &file->H5DWMM->mpi.local_rank);
    file->H5DWMM->H5LS->io_node = (file->H5DWMM->mpi.local_rank == 0); 
    MPI_Comm_size(file->H5DWMM->mpi.node_comm, &file->H5DWMM->mpi.ppn);
    file->H5DWMM->io.num_request = 0; 
    pthread_cond_init(&file->H5DWMM->io.io_cond, NULL);
    pthread_cond_init(&file->H5DWMM->io.master_cond, NULL);
    pthread_mutex_init(&file->H5DWMM->io.request_lock, NULL);
    file->H5DWMM->cache = (LocalStorageCache*)malloc(sizeof(LocalStorageCache));
    file->H5DWMM->cache->mspace_total = size;
    file->H5DWMM->cache->mspace_left = file->H5DWMM->cache->mspace_total;
    file->H5DWMM->cache->mspace_per_rank_total = file->H5DWMM->cache->mspace_total / file->H5DWMM->mpi.ppn;
    file->H5DWMM->cache->mspace_per_rank_left = file->H5DWMM->cache->mspace_per_rank_total;
    file->H5DWMM->cache->purpose = WRITE;
    file->H5DWMM->cache->duration = duration; 
    if (file->H5DWMM->H5LS->storage!=MEMORY) {
      strcpy(file->H5DWMM->cache->path, file->H5DWMM->H5LS->path);
      strcat(file->H5DWMM->cache->path, "/");
      strcat(file->H5DWMM->cache->path, get_fname(name));
      strcat(file->H5DWMM->cache->path, "-cache/");
      mkdir(file->H5DWMM->cache->path, 0755); // setup the folder with the name of the file, and put everything under it.
      if (debug_level() > 0) printf("file-x>H5DWMM->cache-path %s\n", file->H5DWMM->cache->path);
      strcpy(file->H5DWMM->mmap.fname, file->H5DWMM->cache->path);
      char rnd[255];
      sprintf(rnd, "%d", rand());
      strcat(file->H5DWMM->mmap.fname, rnd);
      strcat(file->H5DWMM->mmap.fname, "-"); 
      sprintf(rnd, "%d", file->H5DWMM->mpi.rank);
      strcat(file->H5DWMM->mmap.fname, rnd);
      strcat(file->H5DWMM->mmap.fname, "_mmapf.dat");
      
      if (debug_level()>0 && io_node()==file->H5DWMM->mpi.rank) {
	printf("**Using node local storage as a cache\n");
	printf("**path: %s\n", file->H5DWMM->cache->path);
	printf("**fname: %20s\n", file->H5DWMM->mmap.fname);
      }
      file->H5DWMM->mmap.fd = open(file->H5DWMM->mmap.fname, O_RDWR | O_CREAT | O_TRUNC, 0644);
    } else {
      file->H5DWMM->mmap.buf = malloc(size);
    }
    file->H5DWMM->io.request_list = (thread_data_t*) malloc(sizeof(thread_data_t));
    H5LSregister_cache(file->H5DWMM->H5LS, file->H5DWMM->cache, (void *) file);
    int rc = pthread_create(&file->H5DWMM->io.pthread, NULL, H5Dwrite_pthread_func_vol, file->H5DWMM);
	
    pthread_mutex_lock(&file->H5DWMM->io.request_lock);
    
    file->H5DWMM->io.offset_current = 0;
    file->H5DWMM->mmap.offset = 0;
    
    file->H5DWMM->io.request_list->id = 0; 
    file->H5DWMM->io.current_request = file->H5DWMM->io.request_list; 
    file->H5DWMM->io.first_request = file->H5DWMM->io.request_list; 
    pthread_mutex_unlock(&file->H5DWMM->io.request_lock);
  } else {
    file->read_cache = true; 
    if (file->H5DRMM==NULL)
      file->H5DRMM = (H5Dread_cache_metadata *) malloc(sizeof(H5Dread_cache_metadata));
    else {
      if (file->H5DRMM->mpi.rank == io_node()) printf("file_cache_create: cache data already exist. Remove first!\n");
      return SUCCEED; 
    }
    file->H5DRMM->H5LS = (LocalStorage*) malloc(sizeof(LocalStorage));
    if (H5Pget_fapl_cache(fapl_id, "LOCAL_STORAGE", file->H5DRMM->H5LS)<0) {
      free(file->H5DRMM->H5LS); 
      file->H5DRMM->H5LS=&H5LS;
    } 
    MPI_Comm comm;
    MPI_Info info_mpi;
    H5Pget_fapl_mpio(fapl_id, &comm, &info_mpi);
    MPI_Comm_dup(comm, &file->H5DRMM->mpi.comm);
    MPI_Comm_rank(comm, &file->H5DRMM->mpi.rank);
    MPI_Comm_size(comm, &file->H5DRMM->mpi.nproc);
    MPI_Comm_split_type(file->H5DRMM->mpi.comm, MPI_COMM_TYPE_SHARED, 0, MPI_INFO_NULL, 
			&file->H5DRMM->mpi.node_comm);
    MPI_Comm_rank(file->H5DRMM->mpi.node_comm, &file->H5DRMM->mpi.local_rank);
    file->H5DRMM->H5LS->io_node = (file->H5DRMM->mpi.local_rank==0);
    MPI_Comm_size(file->H5DRMM->mpi.node_comm, &file->H5DRMM->mpi.ppn);
    /* setting up cache within a folder */
    file->H5DRMM->cache = (LocalStorageCache*)malloc(sizeof(LocalStorageCache));
    strcpy(file->H5DRMM->cache->path, file->H5DRMM->H5LS->path);
    strcat(file->H5DRMM->cache->path, "/");
    strcat(file->H5DRMM->cache->path, get_fname(name));
    strcat(file->H5DRMM->cache->path, "-cache/");
    mkdir(file->H5DRMM->cache->path, 0755);
  }
  return SUCCEED; 
}


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_file_create
 *
 * Purpose:     Creates a container using this connector
 *
 * Return:      Success:    Pointer to a file object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_pass_through_ext_file_create(const char *name, unsigned flags, hid_t fcpl_id,
    hid_t fapl_id, hid_t dxpl_id, void **req)
{
    H5VL_pass_through_ext_info_t *info;
    H5VL_pass_through_ext_t *file;
    hid_t under_fapl_id;
    void *under;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL FILE Create\n");
#endif
    /* Get copy of our VOL info from FAPL */
    H5Pget_vol_info(fapl_id, (void **)&info);

    /* Make sure we have info about the underlying VOL to be used */
    if (!info)
        return NULL;

    /* Copy the FAPL */
    under_fapl_id = H5Pcopy(fapl_id);

    /* Set the VOL ID and info for the underlying FAPL */
    H5Pset_vol(under_fapl_id, info->under_vol_id, info->under_vol_info);

    /* Open the file with the underlying VOL connector */
    under = H5VLfile_create(name, flags, fcpl_id, under_fapl_id, dxpl_id, req);

    if(under) {
        file = H5VL_pass_through_ext_new_obj(under, info->under_vol_id);

        /* Check for async request */
        if(req && *req)
            *req = H5VL_pass_through_ext_new_obj(*req, info->under_vol_id);
    } /* end if */
    else
        file = NULL;
    
    file->write_cache = false;
    file->read_cache = false; 
    file->parent = NULL;

    file->H5DRMM=NULL;
    file->H5DWMM=NULL;
    
    hsize_t write_size = HDF5_WRITE_CACHE_SIZE;

    if (getenv("HDF5_CACHE_WR")) {
      if (strcmp(getenv("HDF5_CACHE_WR"), "yes")==0)
	file->write_cache=true;
    } else if (H5Pexist(fapl_id, "HDF5_CACHE_WR")>0) {
      H5Pget(fapl_id, "HDF5_CACHE_WR", &file->write_cache);
    }

    if (getenv("HDF5_CACHE_RD")) {
      if (strcmp(getenv("HDF5_CACHE_RD"), "yes")==0)
	file->read_cache=true;
    } else if (H5Pexist(fapl_id, "HDF5_CACHE_RD")>0) {
      H5Pget(fapl_id, "HDF5_CACHE_RD", &file->read_cache);
    }
    
    if (getenv("HDF5_WRITE_CACHE_SIZE"))
      write_size = HDF5_WRITE_CACHE_SIZE; 
    else if (H5Pget_fapl_cache(fapl_id, "HDF5_WRITE_CACHE_SIZE", &write_size)<0)
      write_size = HDF5_WRITE_CACHE_SIZE;

    if (file->write_cache)
      H5VL_pass_through_ext_file_cache_create((void *) file, name, fapl_id, write_size, 
					      WRITE, PERMANENT);
    if (file->read_cache)
      H5VL_pass_through_ext_file_cache_create((void *) file, name, fapl_id, 0,
					      READ, TEMPORAL);

    /* Close underlying FAPL */
    H5Pclose(under_fapl_id);

    /* Release copy of our VOL info */
    H5VL_pass_through_ext_info_free(info);
    return (void *)file;
} /* end H5VL_pass_through_ext_file_create() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_file_open
 *
 * Purpose:     Opens a container created with this connector
 *
 * Return:      Success:    Pointer to a file object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_pass_through_ext_file_open(const char *name, unsigned flags, hid_t fapl_id,
    hid_t dxpl_id, void **req)
{
    H5VL_pass_through_ext_info_t *info;
    H5VL_pass_through_ext_t *file;
    hid_t under_fapl_id;
    void *under;
    printf("%s:%d: Pass_through VOL is called.\n", __func__, __LINE__);
#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL FILE Open\n");
#endif

    /* Get copy of our VOL info from FAPL */
    H5Pget_vol_info(fapl_id, (void **)&info);

    /* Make sure we have info about the underlying VOL to be used */
    if (!info)
        return NULL;

    /* Copy the FAPL */
    under_fapl_id = H5Pcopy(fapl_id);

    /* Set the VOL ID and info for the underlying FAPL */
    H5Pset_vol(under_fapl_id, info->under_vol_id, info->under_vol_info);

    /* Open the file with the underlying VOL connector */
    under = H5VLfile_open(name, flags, under_fapl_id, dxpl_id, req);
    if(under) {
        file = H5VL_pass_through_ext_new_obj(under, info->under_vol_id);
	
        /* Check for async request */
        if(req && *req)
            *req = H5VL_pass_through_ext_new_obj(*req, info->under_vol_id);
	/* turn on read, only when MPI is initialized. This is to solve some issue in h5dump, h5ls apps */
	int init;
	MPI_Initialized(&init);

	file->write_cache = false;
	file->read_cache = false; 
	file->parent = NULL;
	file->H5DRMM=NULL;
	file->H5DWMM=NULL;
    

	hsize_t write_size = HDF5_WRITE_CACHE_SIZE;
	if (getenv("HDF5_CACHE_WR")) {
	  if (strcmp(getenv("HDF5_CACHE_WR"), "yes")==0)
	    file->write_cache=true;
	} else {
	  H5Pget_fapl_cache(fapl_id, "HDF5_CACHE_WR", &file->write_cache);
	}
	
	if (getenv("HDF5_CACHE_RD")) {
	  if (strcmp(getenv("HDF5_CACHE_RD"), "yes")==0)
	    file->read_cache=true;
	} else {
	  H5Pget_fapl_cache(fapl_id, "HDF5_CACHE_RD", &file->read_cache);
	}

	if (H5Pget_fapl_cache(fapl_id, "HDF5_WRITE_CACHE_SIZE", &write_size)<0)
	  write_size = HDF5_WRITE_CACHE_SIZE; 

	if (file->write_cache)
	  H5VL_pass_through_ext_file_cache_create((void *) file, name, fapl_id, write_size, 
						  WRITE, PERMANENT);
	if (file->read_cache)
	  H5VL_pass_through_ext_file_cache_create((void *) file, name, fapl_id, 0, 
						  READ, TEMPORAL);
    }
    else
      file = NULL;
    /* Close underlying FAPL */
    H5Pclose(under_fapl_id);

    H5VL_pass_through_ext_info_free(info);
    return (void *)file;
} /* end H5VL_pass_through_ext_file_open() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_file_get
 *
 * Purpose:     Get info about a file
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_file_get(void *file, H5VL_file_get_t get_type, hid_t dxpl_id,
    void **req, va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)file;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL FILE Get\n");
#endif

    ret_value = H5VLfile_get(o->under_object, o->under_vol_id, get_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_file_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_file_specific_reissue
 *
 * Purpose:     Re-wrap vararg arguments into a va_list and reissue the
 *              file specific callback to the underlying VOL connector.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_file_specific_reissue(void *obj, hid_t connector_id,
    H5VL_file_specific_t specific_type, hid_t dxpl_id, void **req, ...)
{
    va_list arguments;
    herr_t ret_value;

    va_start(arguments, req);
    ret_value = H5VLfile_specific(obj, connector_id, specific_type, dxpl_id, req, arguments);
    va_end(arguments);

    return ret_value;
} /* end H5VL_pass_through_ext_file_specific_reissue() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_file_specific
 *
 * Purpose:     Specific operation on file
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_file_specific(void *file, H5VL_file_specific_t specific_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)file;
    hid_t under_vol_id = -1;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL FILE Specific\n");
#endif

    /* Unpack arguments to get at the child file pointer when mounting a file */
    if(specific_type == H5VL_FILE_MOUNT) {
        H5I_type_t loc_type;
        const char *name;
        H5VL_pass_through_ext_t *child_file;
        hid_t plist_id;

        /* Retrieve parameters for 'mount' operation, so we can unwrap the child file */
        loc_type = (H5I_type_t)va_arg(arguments, int); /* enum work-around */
        name = va_arg(arguments, const char *);
        child_file = (H5VL_pass_through_ext_t *)va_arg(arguments, void *);
        plist_id = va_arg(arguments, hid_t);

        /* Keep the correct underlying VOL ID for possible async request token */
        under_vol_id = o->under_vol_id;

        /* Re-issue 'file specific' call, using the unwrapped pieces */
        ret_value = H5VL_pass_through_ext_file_specific_reissue(o->under_object, o->under_vol_id, specific_type, dxpl_id, req, (int)loc_type, name, child_file->under_object, plist_id);
    } /* end if */
    else if(specific_type == H5VL_FILE_IS_ACCESSIBLE || specific_type == H5VL_FILE_DELETE) {
        H5VL_pass_through_ext_info_t *info;
        hid_t fapl_id, under_fapl_id;
        const char *name;
        htri_t *ret;

        /* Get the arguments for the 'is accessible' check */
        fapl_id = va_arg(arguments, hid_t);
        name    = va_arg(arguments, const char *);
        ret     = va_arg(arguments, htri_t *);

        /* Get copy of our VOL info from FAPL */
        H5Pget_vol_info(fapl_id, (void **)&info);

        /* Make sure we have info about the underlying VOL to be used */
        if (!info)
            return (-1);

        /* Copy the FAPL */
        under_fapl_id = H5Pcopy(fapl_id);

        /* Set the VOL ID and info for the underlying FAPL */
        H5Pset_vol(under_fapl_id, info->under_vol_id, info->under_vol_info);

        /* Keep the correct underlying VOL ID for possible async request token */
        under_vol_id = info->under_vol_id;

        /* Re-issue 'file specific' call */
        ret_value = H5VL_pass_through_ext_file_specific_reissue(NULL, info->under_vol_id, specific_type, dxpl_id, req, under_fapl_id, name, ret);

        /* Close underlying FAPL */
        H5Pclose(under_fapl_id);

        /* Release copy of our VOL info */
        H5VL_pass_through_ext_info_free(info);
    } /* end else-if */
    else {
        va_list my_arguments;

        /* Make a copy of the argument list for later, if reopening */
        if(specific_type == H5VL_FILE_REOPEN)
            va_copy(my_arguments, arguments);

        /* Keep the correct underlying VOL ID for possible async request token */
        under_vol_id = o->under_vol_id;

        ret_value = H5VLfile_specific(o->under_object, o->under_vol_id, specific_type, dxpl_id, req, arguments);

        /* Wrap file struct pointer, if we reopened one */
        if(specific_type == H5VL_FILE_REOPEN) {
            if(ret_value >= 0) {
                void      **ret = va_arg(my_arguments, void **);

                if(ret && *ret)
                    *ret = H5VL_pass_through_ext_new_obj(*ret, o->under_vol_id);
            } /* end if */

            /* Finish use of copied vararg list */
            va_end(my_arguments);
        } /* end if */
    } /* end else */

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_file_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_file_optional
 *
 * Purpose:     Perform a connector-specific operation on a file
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_file_optional(void *file, H5VL_file_optional_t opt_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)file;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL File Optional\n");
#endif
    assert(-1!=H5VL_passthru_file_cache_create_op_g);
    assert(-1!=H5VL_passthru_file_cache_remove_op_g);
    if (opt_type == H5VL_passthru_file_cache_create_op_g) {
      hid_t fapl_id = va_arg(arguments, hid_t); 
      hsize_t size = va_arg(arguments, hsize_t);
      cache_purpose_t purpose = va_arg(arguments, cache_purpose_t);
      cache_duration_t duration = va_arg(arguments, cache_duration_t);
      if (purpose==WRITE) 
	o->write_cache = true;
      else if (purpose==READ)
	o->read_cache = true;
      else if (purpose==RDWR) {
	o->read_cache = true; o->write_cache = true; 
      }
      hsize_t size_f;
      char name[255];
      file_get_wrapper(o->under_object, o->under_vol_id, H5VL_FILE_GET_NAME, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL, (int)H5I_FILE, size_f, name, &ret_value);      
      if (o->write_cache && o->read_cache)
	ret_value = H5VL_pass_through_ext_file_cache_create(file, name, fapl_id, size,  purpose, duration);
    } else if (opt_type == H5VL_passthru_file_cache_remove_op_g) {
      H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)file;
      if (o->write_cache && o->H5DWMM!=NULL) {
	ret_value = H5LSremove_cache(o->H5DWMM->H5LS, o->H5DWMM->cache);
	o->write_cache = false; // set it to be false
	free(o->H5DWMM); 
      }
      else if (o->read_cache && o->H5DRMM!=NULL) {
	ret_value = H5LSremove_cache(o->H5DRMM->H5LS, o->H5DRMM->cache);
	o->read_cache = false; // set it to be false
	free(o->H5DRMM);
      }
    }
    else {
      ret_value = H5VLfile_optional(o->under_object, o->under_vol_id, opt_type, dxpl_id, req, arguments);
    }
    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_file_optional() */



/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_file_close
 *
 * Purpose:     Closes a file.
 *
 * Return:      Success:    0
 *              Failure:    -1, file not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_file_close(void *file, hid_t dxpl_id, void **req)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)file;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL FILE Close\n");
#endif
    if (o->write_cache) {
      H5TSmutex_release();
      pthread_mutex_lock(&o->H5DWMM->io.request_lock);
      bool empty = (o->H5DWMM->io.num_request>0);
      pthread_mutex_unlock(&o->H5DWMM->io.request_lock);
      while(!empty)  {
	pthread_mutex_lock(&o->H5DWMM->io.request_lock);
	pthread_cond_signal(&o->H5DWMM->io.io_cond);
	pthread_cond_wait(&o->H5DWMM->io.master_cond, &o->H5DWMM->io.request_lock);
	empty = (o->H5DWMM->io.num_request==0);
	pthread_mutex_unlock(&o->H5DWMM->io.request_lock);
      }
      pthread_mutex_lock(&o->H5DWMM->io.request_lock);
      o->H5DWMM->io.num_request=-1;
      pthread_cond_signal(&o->H5DWMM->io.io_cond);
      pthread_mutex_unlock(&o->H5DWMM->io.request_lock);
      pthread_join(o->H5DWMM->io.pthread, NULL);
      if (o->H5DWMM->H5LS->storage!=MEMORY)
	close(o->H5DWMM->mmap.fd);
      o->H5DWMM->cache->mspace_left = o->H5DWMM->cache->mspace_total;
      hbool_t acq = false;
      while(!acq)
	H5TSmutex_acquire(&acq);
      if (H5LSremove_cache(o->H5DWMM->H5LS, o->H5DWMM->cache)!=SUCCEED) 
	printf(" Could not remove cache %s\n", o->H5DWMM->cache->path);
      free(o->H5DWMM);
      o->H5DWMM=NULL; 
    }
    if (o->read_cache) {
      if (o->H5DRMM->H5LS->io_node)
	rmdir(o->H5DRMM->cache->path); // remove the file 
      free(o->H5DRMM);
      o->H5DRMM=NULL;
    }
    ret_value = H5VLfile_close(o->under_object, o->under_vol_id, dxpl_id, req);
    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);

    /* Release our wrapper, if underlying file was closed */
    if(ret_value >= 0)
        H5VL_pass_through_ext_free_obj(o);

    return ret_value;
} /* end H5VL_pass_through_ext_file_close() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_group_create
 *
 * Purpose:     Creates a group inside a container
 *
 * Return:      Success:    Pointer to a group object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_pass_through_ext_group_create(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id,
    hid_t dxpl_id, void **req)
{
    H5VL_pass_through_ext_t *group;
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    void *under;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL GROUP Create\n");
#endif

    under = H5VLgroup_create(o->under_object, loc_params, o->under_vol_id, name, lcpl_id, gcpl_id,  gapl_id, dxpl_id, req);
    if(under) {
        group = H5VL_pass_through_ext_new_obj(under, o->under_vol_id);
	  /* passing the cache information on from file to group */
	group->write_cache = o->write_cache;
	group->H5DWMM = o->H5DWMM; 
	group->read_cache = o->read_cache; 
	group->H5DRMM = o->H5DRMM;
	group->parent = obj; 
        /* Check for async request */
        if(req && *req)
            *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);
    } /* end if */
    else
        group = NULL;

    return (void *)group;
} /* end H5VL_pass_through_ext_group_create() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_group_open
 *
 * Purpose:     Opens a group inside a container
 *
 * Return:      Success:    Pointer to a group object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_pass_through_ext_group_open(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    H5VL_pass_through_ext_t *group;
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    void *under;
    printf("%s:%d: Pass_through VOL is called.\n", __func__, __LINE__);
#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL GROUP Open\n");
#endif

    under = H5VLgroup_open(o->under_object, loc_params, o->under_vol_id, name, gapl_id, dxpl_id, req);
    if(under) {
        group = H5VL_pass_through_ext_new_obj(under, o->under_vol_id);
	group->parent = obj; 
        /* Check for async request */
        if(req && *req)
            *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);
		group->read_cache = o->read_cache; 
        group->write_cache = o->write_cache; 
		group->H5DRMM = o->H5DRMM; 
        group->H5DWMM = o->H5DWMM; 
    } /* end if */
    else
      group = NULL;

    return (void *)group;
} /* end H5VL_pass_through_ext_group_open() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_group_get
 *
 * Purpose:     Get info about a group
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_group_get(void *obj, H5VL_group_get_t get_type, hid_t dxpl_id,
    void **req, va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL GROUP Get\n");
#endif

    ret_value = H5VLgroup_get(o->under_object, o->under_vol_id, get_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_group_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_group_specific
 *
 * Purpose:     Specific operation on a group
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_group_specific(void *obj, H5VL_group_specific_t specific_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    hid_t under_vol_id;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL GROUP Specific\n");
#endif

    // Save copy of underlying VOL connector ID and prov helper, in case of
    // refresh destroying the current object
    under_vol_id = o->under_vol_id;

    ret_value = H5VLgroup_specific(o->under_object, o->under_vol_id, specific_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_group_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_group_optional
 *
 * Purpose:     Perform a connector-specific operation on a group
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_group_optional(void *obj, H5VL_group_optional_t opt_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL GROUP Optional\n");
#endif
    /* Sanity check */
    assert(-1 != H5VL_passthru_group_fiddle_op_g);

    /* Capture and perform connector-specific 'fiddle' operation */
    if(opt_type == H5VL_passthru_group_fiddle_op_g) {
printf("fiddle\n");

        /* <do 'fiddle'> */

        /* Set return value */
        ret_value = 0;

    } else
      ret_value = H5VLgroup_optional(o->under_object, o->under_vol_id, opt_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_group_optional() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_group_close
 *
 * Purpose:     Closes a group.
 *
 * Return:      Success:    0
 *              Failure:    -1, group not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_group_close(void *grp, hid_t dxpl_id, void **req)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)grp;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL GROUP Close\n");
#endif

    ret_value = H5VLgroup_close(o->under_object, o->under_vol_id, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);

    /* Release our wrapper, if underlying file was closed */
    if(ret_value >= 0)
        H5VL_pass_through_ext_free_obj(o);

    return ret_value;
} /* end H5VL_pass_through_ext_group_close() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_link_create_reissue
 *
 * Purpose:     Re-wrap vararg arguments into a va_list and reissue the
 *              link create callback to the underlying VOL connector.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_link_create_reissue(H5VL_link_create_type_t create_type,
    void *obj, const H5VL_loc_params_t *loc_params, hid_t connector_id,
    hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req, ...)
{
    va_list arguments;
    herr_t ret_value;

    va_start(arguments, req);
    ret_value = H5VLlink_create(create_type, obj, loc_params, connector_id, lcpl_id, lapl_id, dxpl_id, req, arguments);
    va_end(arguments);
    
    return ret_value;
} /* end H5VL_pass_through_ext_link_create_reissue() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_link_create
 *
 * Purpose:     Creates a hard / soft / UD / external link.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_link_create(H5VL_link_create_type_t create_type, void *obj,
    const H5VL_loc_params_t *loc_params, hid_t lcpl_id, hid_t lapl_id,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    hid_t under_vol_id = -1;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL LINK Create\n");
#endif

    /* Try to retrieve the "under" VOL id */
    if(o)
        under_vol_id = o->under_vol_id;

    /* Fix up the link target object for hard link creation */
    if(H5VL_LINK_CREATE_HARD == create_type) {
        void         *cur_obj;
        H5VL_loc_params_t *cur_params;

        /* Retrieve the object & loc params for the link target */
        cur_obj = va_arg(arguments, void *);
        cur_params = va_arg(arguments, H5VL_loc_params_t *);

        /* If it's a non-NULL pointer, find the 'under object' and re-set the property */
        if(cur_obj) {
            /* Check if we still need the "under" VOL ID */
            if(under_vol_id < 0)
                under_vol_id = ((H5VL_pass_through_ext_t *)cur_obj)->under_vol_id;

            /* Set the object for the link target */
            cur_obj = ((H5VL_pass_through_ext_t *)cur_obj)->under_object;
        } /* end if */

        /* Re-issue 'link create' call, using the unwrapped pieces */
        ret_value = H5VL_pass_through_ext_link_create_reissue(create_type, (o ? o->under_object : NULL), loc_params, under_vol_id, lcpl_id, lapl_id, dxpl_id, req, cur_obj, cur_params);
    } /* end if */
    else
        ret_value = H5VLlink_create(create_type, (o ? o->under_object : NULL), loc_params, under_vol_id, lcpl_id, lapl_id, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_link_create() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_link_copy
 *
 * Purpose:     Renames an object within an HDF5 container and copies it to a new
 *              group.  The original name SRC is unlinked from the group graph
 *              and then inserted with the new name DST (which can specify a
 *              new path for the object) as an atomic operation. The names
 *              are interpreted relative to SRC_LOC_ID and
 *              DST_LOC_ID, which are either file IDs or group ID.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_link_copy(void *src_obj, const H5VL_loc_params_t *loc_params1,
    void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t lcpl_id,
    hid_t lapl_id, hid_t dxpl_id, void **req)
{
    H5VL_pass_through_ext_t *o_src = (H5VL_pass_through_ext_t *)src_obj;
    H5VL_pass_through_ext_t *o_dst = (H5VL_pass_through_ext_t *)dst_obj;
    hid_t under_vol_id = -1;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL LINK Copy\n");
#endif

    /* Retrieve the "under" VOL id */
    if(o_src)
        under_vol_id = o_src->under_vol_id;
    else if(o_dst)
        under_vol_id = o_dst->under_vol_id;
    assert(under_vol_id > 0);

    ret_value = H5VLlink_copy((o_src ? o_src->under_object : NULL), loc_params1, (o_dst ? o_dst->under_object : NULL), loc_params2, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_link_copy() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_link_move
 *
 * Purpose:     Moves a link within an HDF5 file to a new group.  The original
 *              name SRC is unlinked from the group graph
 *              and then inserted with the new name DST (which can specify a
 *              new path for the object) as an atomic operation. The names
 *              are interpreted relative to SRC_LOC_ID and
 *              DST_LOC_ID, which are either file IDs or group ID.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_link_move(void *src_obj, const H5VL_loc_params_t *loc_params1,
    void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t lcpl_id,
    hid_t lapl_id, hid_t dxpl_id, void **req)
{
    H5VL_pass_through_ext_t *o_src = (H5VL_pass_through_ext_t *)src_obj;
    H5VL_pass_through_ext_t *o_dst = (H5VL_pass_through_ext_t *)dst_obj;
    hid_t under_vol_id = -1;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL LINK Move\n");
#endif

    /* Retrieve the "under" VOL id */
    if(o_src)
        under_vol_id = o_src->under_vol_id;
    else if(o_dst)
        under_vol_id = o_dst->under_vol_id;
    assert(under_vol_id > 0);

    ret_value = H5VLlink_move((o_src ? o_src->under_object : NULL), loc_params1, (o_dst ? o_dst->under_object : NULL), loc_params2, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_link_move() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_link_get
 *
 * Purpose:     Get info about a link
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_link_get(void *obj, const H5VL_loc_params_t *loc_params,
    H5VL_link_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL LINK Get\n");
#endif

    ret_value = H5VLlink_get(o->under_object, loc_params, o->under_vol_id, get_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_link_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_link_specific
 *
 * Purpose:     Specific operation on a link
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_link_specific(void *obj, const H5VL_loc_params_t *loc_params,
    H5VL_link_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL LINK Specific\n");
#endif

    ret_value = H5VLlink_specific(o->under_object, loc_params, o->under_vol_id, specific_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_link_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_link_optional
 *
 * Purpose:     Perform a connector-specific operation on a link
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_link_optional(void *obj, H5VL_link_optional_t opt_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL LINK Optional\n");
#endif

    ret_value = H5VLlink_optional(o->under_object, o->under_vol_id, opt_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_link_optional() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_object_open
 *
 * Purpose:     Opens an object inside a container.
 *
 * Return:      Success:    Pointer to object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_pass_through_ext_object_open(void *obj, const H5VL_loc_params_t *loc_params,
    H5I_type_t *opened_type, hid_t dxpl_id, void **req)
{
    H5VL_pass_through_ext_t *new_obj;
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    void *under;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL OBJECT Open\n");
#endif
    under = H5VLobject_open(o->under_object, loc_params, o->under_vol_id, opened_type, dxpl_id, req);
    
    if(under) {
        new_obj = H5VL_pass_through_ext_new_obj(under, o->under_vol_id);
	if (*opened_type==H5I_GROUP) { // if group is opened 
	  new_obj->read_cache = o->read_cache;
	  new_obj->write_cache = o->write_cache;
	  new_obj->H5DWMM = o->H5DWMM;
	  new_obj->H5DRMM = o->H5DRMM;
	} else if (*opened_type==H5I_DATASET) { // if dataset is opened
	  new_obj->read_cache = o->read_cache;
	  new_obj->write_cache = o->write_cache;
	  new_obj->H5DRMM = o->H5DRMM;
	  new_obj->H5DWMM = o->H5DWMM; 
	  int called = 0;
	  MPI_Initialized(&called);
	  if (called && new_obj->read_cache)
	    H5VL_pass_through_ext_dataset_read_cache_create((void*) new_obj, loc_params->loc_data.loc_by_name.name);
	}
        /* Check for async request */
        if(req && *req)
            *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);
    } /* end if */
    else
        new_obj = NULL;

    return (void *)new_obj;
} /* end H5VL_pass_through_ext_object_open() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_object_copy
 *
 * Purpose:     Copies an object inside a container.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_object_copy(void *src_obj, const H5VL_loc_params_t *src_loc_params,
    const char *src_name, void *dst_obj, const H5VL_loc_params_t *dst_loc_params,
    const char *dst_name, hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id,
    void **req)
{
    H5VL_pass_through_ext_t *o_src = (H5VL_pass_through_ext_t *)src_obj;
    H5VL_pass_through_ext_t *o_dst = (H5VL_pass_through_ext_t *)dst_obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL OBJECT Copy\n");
#endif

    ret_value = H5VLobject_copy(o_src->under_object, src_loc_params, src_name, o_dst->under_object, dst_loc_params, dst_name, o_src->under_vol_id, ocpypl_id, lcpl_id, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o_src->under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_object_copy() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_object_get
 *
 * Purpose:     Get info about an object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_object_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL OBJECT Get\n");
#endif

    ret_value = H5VLobject_get(o->under_object, loc_params, o->under_vol_id, get_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_object_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_object_specific
 *
 * Purpose:     Specific operation on an object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_object_specific(void *obj, const H5VL_loc_params_t *loc_params,
    H5VL_object_specific_t specific_type, hid_t dxpl_id, void **req,
    va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    hid_t under_vol_id;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL OBJECT Specific\n");
#endif

    // Save copy of underlying VOL connector ID and prov helper, in case of
    // refresh destroying the current object
    under_vol_id = o->under_vol_id;

    ret_value = H5VLobject_specific(o->under_object, loc_params, o->under_vol_id, specific_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_object_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_object_optional
 *
 * Purpose:     Perform a connector-specific operation for an object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_object_optional(void *obj, H5VL_object_optional_t opt_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL OBJECT Optional\n");
#endif

    ret_value = H5VLobject_optional(o->under_object, o->under_vol_id, opt_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = H5VL_pass_through_ext_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_pass_through_ext_object_optional() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_introspect_get_conn_clss
 *
 * Purpose:     Query the connector class.
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_pass_through_ext_introspect_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl,
    const H5VL_class_t **conn_cls)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL INTROSPECT GetConnCls\n");
#endif

    /* Check for querying this connector's class */
    if(H5VL_GET_CONN_LVL_CURR == lvl) {
        *conn_cls = &H5VL_pass_through_ext_g;
        ret_value = 0;
    } /* end if */
    else
        ret_value = H5VLintrospect_get_conn_cls(o->under_object, o->under_vol_id,
            lvl, conn_cls);

    return ret_value;
} /* end H5VL_pass_through_ext_introspect_get_conn_cls() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_introspect_opt_query
 *
 * Purpose:     Query if an optional operation is supported by this connector
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_pass_through_ext_introspect_opt_query(void *obj, H5VL_subclass_t cls,
    int opt_type, hbool_t *supported)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL INTROSPECT OptQuery\n");
#endif

    ret_value = H5VLintrospect_opt_query(o->under_object, o->under_vol_id, cls,
        opt_type, supported);

    return ret_value;
} /* end H5VL_pass_through_ext_introspect_opt_query() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_request_wait
 *
 * Purpose:     Wait (with a timeout) for an async operation to complete
 *
 * Note:        Releases the request if the operation has completed and the
 *              connector callback succeeds
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_request_wait(void *obj, uint64_t timeout,
    H5ES_status_t *status)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL REQUEST Wait\n");
#endif

    ret_value = H5VLrequest_wait(o->under_object, o->under_vol_id, timeout, status);

    if(ret_value >= 0 && *status != H5ES_STATUS_IN_PROGRESS)
        H5VL_pass_through_ext_free_obj(o);

    return ret_value;
} /* end H5VL_pass_through_ext_request_wait() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_request_notify
 *
 * Purpose:     Registers a user callback to be invoked when an asynchronous
 *              operation completes
 *
 * Note:        Releases the request, if connector callback succeeds
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_request_notify(void *obj, H5VL_request_notify_t cb, void *ctx)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL REQUEST Notify\n");
#endif

    ret_value = H5VLrequest_notify(o->under_object, o->under_vol_id, cb, ctx);

    if(ret_value >= 0)
        H5VL_pass_through_ext_free_obj(o);

    return ret_value;
} /* end H5VL_pass_through_ext_request_notify() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_request_cancel
 *
 * Purpose:     Cancels an asynchronous operation
 *
 * Note:        Releases the request, if connector callback succeeds
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_request_cancel(void *obj)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL REQUEST Cancel\n");
#endif

    ret_value = H5VLrequest_cancel(o->under_object, o->under_vol_id);

    if(ret_value >= 0)
        H5VL_pass_through_ext_free_obj(o);

    return ret_value;
} /* end H5VL_pass_through_ext_request_cancel() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_request_specific_reissue
 *
 * Purpose:     Re-wrap vararg arguments into a va_list and reissue the
 *              request specific callback to the underlying VOL connector.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_request_specific_reissue(void *obj, hid_t connector_id,
    H5VL_request_specific_t specific_type, ...)
{
    va_list arguments;
    herr_t ret_value;

    va_start(arguments, specific_type);
    ret_value = H5VLrequest_specific(obj, connector_id, specific_type, arguments);
    va_end(arguments);

    return ret_value;
} /* end H5VL_pass_through_ext_request_specific_reissue() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_request_specific
 *
 * Purpose:     Specific operation on a request
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_request_specific(void *obj, H5VL_request_specific_t specific_type,
    va_list arguments)
{
    herr_t ret_value = -1;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL REQUEST Specific\n");
#endif

    if(H5VL_REQUEST_WAITANY == specific_type ||
            H5VL_REQUEST_WAITSOME == specific_type ||
            H5VL_REQUEST_WAITALL == specific_type) {
        va_list tmp_arguments;
        size_t req_count;

        /* Sanity check */
        assert(obj == NULL);

        /* Get enough info to call the underlying connector */
        va_copy(tmp_arguments, arguments);
        req_count = va_arg(tmp_arguments, size_t);

        /* Can only use a request to invoke the underlying VOL connector when there's >0 requests */
        if(req_count > 0) {
            void **req_array;
            void **under_req_array;
            uint64_t timeout;
            H5VL_pass_through_ext_t *o;
            size_t u;               /* Local index variable */

            /* Get the request array */
            req_array = va_arg(tmp_arguments, void **);

            /* Get a request to use for determining the underlying VOL connector */
            o = (H5VL_pass_through_ext_t *)req_array[0];

            /* Create array of underlying VOL requests */
            under_req_array = (void **)malloc(req_count * sizeof(void **));
            for(u = 0; u < req_count; u++)
                under_req_array[u] = ((H5VL_pass_through_ext_t *)req_array[u])->under_object;

            /* Remove the timeout value from the vararg list (it's used in all the calls below) */
            timeout = va_arg(tmp_arguments, uint64_t);

            /* Release requests that have completed */
            if(H5VL_REQUEST_WAITANY == specific_type) {
                size_t *idx;          /* Pointer to the index of completed request */
                H5ES_status_t *status;  /* Pointer to the request's status */

                /* Retrieve the remaining arguments */
                idx = va_arg(tmp_arguments, size_t *);
                assert(*idx <= req_count);
                status = va_arg(tmp_arguments, H5ES_status_t *);

                /* Reissue the WAITANY 'request specific' call */
                ret_value = H5VL_pass_through_ext_request_specific_reissue(o->under_object, o->under_vol_id, specific_type, req_count, under_req_array, timeout,
                                                                       idx,
                                                                       status);

                /* Release the completed request, if it completed */
                if(ret_value >= 0 && *status != H5ES_STATUS_IN_PROGRESS) {
                    H5VL_pass_through_ext_t *tmp_o;

                    tmp_o = (H5VL_pass_through_ext_t *)req_array[*idx];
                    H5VL_pass_through_ext_free_obj(tmp_o);
                } /* end if */
            } /* end if */
            else if(H5VL_REQUEST_WAITSOME == specific_type) {
                size_t *outcount;               /* # of completed requests */
                unsigned *array_of_indices;     /* Array of indices for completed requests */
                H5ES_status_t *array_of_statuses; /* Array of statuses for completed requests */

                /* Retrieve the remaining arguments */
                outcount = va_arg(tmp_arguments, size_t *);
                assert(*outcount <= req_count);
                array_of_indices = va_arg(tmp_arguments, unsigned *);
                array_of_statuses = va_arg(tmp_arguments, H5ES_status_t *);

                /* Reissue the WAITSOME 'request specific' call */
                ret_value = H5VL_pass_through_ext_request_specific_reissue(o->under_object, o->under_vol_id, specific_type, req_count, under_req_array, timeout, outcount, array_of_indices, array_of_statuses);

                /* If any requests completed, release them */
                if(ret_value >= 0 && *outcount > 0) {
                    unsigned *idx_array;    /* Array of indices of completed requests */

                    /* Retrieve the array of completed request indices */
                    idx_array = va_arg(tmp_arguments, unsigned *);

                    /* Release the completed requests */
                    for(u = 0; u < *outcount; u++) {
                        H5VL_pass_through_ext_t *tmp_o;

                        tmp_o = (H5VL_pass_through_ext_t *)req_array[idx_array[u]];
                        H5VL_pass_through_ext_free_obj(tmp_o);
                    } /* end for */
                } /* end if */
            } /* end else-if */
            else {      /* H5VL_REQUEST_WAITALL == specific_type */
                H5ES_status_t *array_of_statuses; /* Array of statuses for completed requests */

                /* Retrieve the remaining arguments */
                array_of_statuses = va_arg(tmp_arguments, H5ES_status_t *);

                /* Reissue the WAITALL 'request specific' call */
                ret_value = H5VL_pass_through_ext_request_specific_reissue(o->under_object, o->under_vol_id, specific_type, req_count, under_req_array, timeout, array_of_statuses);

                /* Release the completed requests */
                if(ret_value >= 0) {
                    for(u = 0; u < req_count; u++) {
                        if(array_of_statuses[u] != H5ES_STATUS_IN_PROGRESS) {
                            H5VL_pass_through_ext_t *tmp_o;

                            tmp_o = (H5VL_pass_through_ext_t *)req_array[u];
                            H5VL_pass_through_ext_free_obj(tmp_o);
                        } /* end if */
                    } /* end for */
                } /* end if */
            } /* end else */

            /* Release array of requests for underlying connector */
            free(under_req_array);
        } /* end if */

        /* Finish use of copied vararg list */
        va_end(tmp_arguments);
    } /* end if */
    else
        assert(0 && "Unknown 'specific' operation");

    return ret_value;
} /* end H5VL_pass_through_ext_request_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_request_optional
 *
 * Purpose:     Perform a connector-specific operation for a request
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_request_optional(void *obj, H5VL_request_optional_t opt_type,
    va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL REQUEST Optional\n");
#endif

    ret_value = H5VLrequest_optional(o->under_object, o->under_vol_id, opt_type, arguments);

    return ret_value;
} /* end H5VL_pass_through_ext_request_optional() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_request_free
 *
 * Purpose:     Releases a request, allowing the operation to complete without
 *              application tracking
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_request_free(void *obj)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL REQUEST Free\n");
#endif

    ret_value = H5VLrequest_free(o->under_object, o->under_vol_id);

    if(ret_value >= 0)
        H5VL_pass_through_ext_free_obj(o);

    return ret_value;
} /* end H5VL_pass_through_ext_request_free() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_blob_put
 *
 * Purpose:     Handles the blob 'put' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_pass_through_ext_blob_put(void *obj, const void *buf, size_t size,
    void *blob_id, void *ctx)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL BLOB Put\n");
#endif

    ret_value = H5VLblob_put(o->under_object, o->under_vol_id, buf, size,
        blob_id, ctx);

    return ret_value;
} /* end H5VL_pass_through_ext_blob_put() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_blob_get
 *
 * Purpose:     Handles the blob 'get' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_pass_through_ext_blob_get(void *obj, const void *blob_id, void *buf,
    size_t size, void *ctx)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL BLOB Get\n");
#endif

    ret_value = H5VLblob_get(o->under_object, o->under_vol_id, blob_id, buf,
        size, ctx);

    return ret_value;
} /* end H5VL_pass_through_ext_blob_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_blob_specific
 *
 * Purpose:     Handles the blob 'specific' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_pass_through_ext_blob_specific(void *obj, void *blob_id,
    H5VL_blob_specific_t specific_type, va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL BLOB Specific\n");
#endif

    ret_value = H5VLblob_specific(o->under_object, o->under_vol_id, blob_id,
        specific_type, arguments);

    return ret_value;
} /* end H5VL_pass_through_ext_blob_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_blob_optional
 *
 * Purpose:     Handles the blob 'optional' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_pass_through_ext_blob_optional(void *obj, void *blob_id,
    H5VL_blob_optional_t opt_type, va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL BLOB Optional\n");
#endif

    ret_value = H5VLblob_optional(o->under_object, o->under_vol_id, blob_id,
        opt_type, arguments);

    return ret_value;
} /* end H5VL_pass_through_ext_blob_optional() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_token_cmp
 *
 * Purpose:     Compare two of the connector's object tokens, setting
 *              *cmp_value, following the same rules as strcmp().
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_token_cmp(void *obj, const H5O_token_t *token1,
    const H5O_token_t *token2, int *cmp_value)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL TOKEN Compare\n");
#endif

    /* Sanity checks */
    assert(obj);
    assert(token1);
    assert(token2);
    assert(cmp_value);

    ret_value = H5VLtoken_cmp(o->under_object, o->under_vol_id, token1, token2, cmp_value);

    return ret_value;
} /* end H5VL_pass_through_ext_token_cmp() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_token_to_str
 *
 * Purpose:     Serialize the connector's object token into a string.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_token_to_str(void *obj, H5I_type_t obj_type,
    const H5O_token_t *token, char **token_str)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL TOKEN To string\n");
#endif

    /* Sanity checks */
    assert(obj);
    assert(token);
    assert(token_str);

    ret_value = H5VLtoken_to_str(o->under_object, obj_type, o->under_vol_id, token, token_str);

    return ret_value;
} /* end H5VL_pass_through_ext_token_to_str() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_token_from_str
 *
 * Purpose:     Deserialize the connector's object token from a string.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_pass_through_ext_token_from_str(void *obj, H5I_type_t obj_type,
    const char *token_str, H5O_token_t *token)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL TOKEN From string\n");
#endif

    /* Sanity checks */
    assert(obj);
    assert(token);
    assert(token_str);

    ret_value = H5VLtoken_from_str(o->under_object, obj_type, o->under_vol_id, token_str, token);

    return ret_value;
} /* end H5VL_pass_through_ext_token_from_str() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_pass_through_ext_optional
 *
 * Purpose:     Handles the generic 'optional' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_pass_through_ext_optional(void *obj, int op_type, hid_t dxpl_id, void **req,
    va_list arguments)
{
    H5VL_pass_through_ext_t *o = (H5VL_pass_through_ext_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL generic Optional\n");
#endif

    ret_value = H5VLoptional(o->under_object, o->under_vol_id, op_type,
        dxpl_id, req, arguments);

    return ret_value;
} /* end H5VL_pass_through_ext_optional() */

