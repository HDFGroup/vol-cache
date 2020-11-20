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
 * Purpose:	The public header file for the pass-through VOL connector.
 */

#ifndef _H5VLcache_ext_H
#define _H5VLcache_ext_H
#include "H5Dio_cache.h"
/* Public headers needed by this file */
#include "H5VLpublic.h"        /* Virtual Object Layer                 */

/* Identifier for the pass-through VOL connector */
#define H5VL_CACHE_EXT	(H5VL_cache_ext_register())

/* Characteristics of the pass-through VOL connector */
#define H5VL_CACHE_EXT_NAME        "cache_ext"
#define H5VL_CACHE_EXT_VALUE       518           /* VOL connector ID */
#define H5VL_CACHE_EXT_VERSION     0


#ifndef H5VL_STRUCT
#define H5VL_STRUCT
/* Pass-through VOL connector info */
typedef struct H5VL_cache_ext_info_t {
    hid_t under_vol_id;         /* VOL ID for under VOL */
    void *under_vol_info;       /* VOL info for under VOL */
} H5VL_cache_ext_info_t;

/* The pass through VOL info object */
typedef struct H5VL_cache_ext_t {
    hid_t  under_vol_id;        /* ID for underlying VOL connector */
    void   *under_object;       /* Info object for underlying VOL connector */
    H5Dread_cache_metadata *H5DRMM;
    H5Dwrite_cache_metadata *H5DWMM;
    bool read_cache;
    bool read_cache_info_set; 
    bool write_cache;
    bool write_cache_info_set; 
    int num_request_dataset;
    void *parent; 
} H5VL_cache_ext_t;

/* The cache VOL wrapper context */
typedef struct H5VL_cache_ext_wrap_ctx_t {
    hid_t under_vol_id;         /* VOL ID for under VOL */
    void *under_wrap_ctx;       /* Object wrapping context for under VOL */
} H5VL_cache_ext_wrap_ctx_t;
#endif

#define H5P_LOCAL_STORAGE_CREATE (H5OPEN H5P_CLS_LOCAL_STORAGE_CREATE_ID_g)
H5_DLLVAR hid_t H5P_CLS_LOCAL_STORAGE_CREATE_ID_g; 


#endif /* _H5VLcache_H */

