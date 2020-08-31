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

#ifndef _H5VLpassthru_ext_H
#define _H5VLpassthru_ext_H
#include "H5Dio_cache.h"
/* Public headers needed by this file */
#include "H5VLpublic.h"        /* Virtual Object Layer                 */

/* Identifier for the pass-through VOL connector */
#define H5VL_PASSTHRU_EXT	(H5VL_pass_through_ext_register())

/* Characteristics of the pass-through VOL connector */
#define H5VL_PASSTHRU_EXT_NAME        "pass_through_ext"
#define H5VL_PASSTHRU_EXT_VALUE       517           /* VOL connector ID */
#define H5VL_PASSTHRU_EXT_VERSION     0


#ifndef H5VL_STRUCT
#define H5VL_STRUCT
/* Pass-through VOL connector info */
typedef struct H5VL_pass_through_ext_info_t {
    hid_t under_vol_id;         /* VOL ID for under VOL */
    void *under_vol_info;       /* VOL info for under VOL */
} H5VL_pass_through_ext_info_t;

/* The pass through VOL info object */
typedef struct H5VL_pass_through_ext_t {
    hid_t  under_vol_id;        /* ID for underlying VOL connector */
    void   *under_object;       /* Info object for underlying VOL connector */
    H5Dread_cache_metadata *H5DRMM;
    H5Dwrite_cache_metadata *H5DWMM;
    bool read_cache;
    bool read_cache_info_set; 
    bool write_cache;
    bool write_cache_info_set; 
    int num_request_dataset; 
} H5VL_pass_through_ext_t;

/* The pass through VOL wrapper context */
typedef struct H5VL_pass_through_ext_wrap_ctx_t {
    hid_t under_vol_id;         /* VOL ID for under VOL */
    void *under_wrap_ctx;       /* Object wrapping context for under VOL */
} H5VL_pass_through_ext_wrap_ctx_t;
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* New "public" API routines */
herr_t H5Dfoo(hid_t dset_id, hid_t dxpl_id, void **req, int i, double d);
herr_t H5Dread_to_cache(hid_t dset_id, hid_t mem_type_id, hid_t memspace_id, hid_t file_space_id, hid_t dxpl_id, void *buf);
herr_t H5Dread_from_cache(hid_t dset_id, hid_t mem_type_id, hid_t memspace_id, hid_t file_space_id, hid_t dxpl_id, void *buf);
herr_t H5Dbar(hid_t dset_id, hid_t dxpl_id, void **req, double *dp, unsigned *up);
herr_t H5Gfiddle(hid_t group_id, hid_t dxpl_id, void **req);
herr_t H5Freserve_cache(hid_t file_id, hid_t hid_dxpl_id, void **req, hsize_t size, cache_purpose_t purpose, cache_duration_t duration);
herr_t H5Fquery_cache(hid_t file_id, hid_t hid_dxpl_id, void **req, hsize_t *size);
H5_DLL hid_t H5VL_pass_through_ext_register(void);

#ifdef __cplusplus
}
#endif

#endif /* _H5VLpassthru_H */

