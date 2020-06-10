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

/* Public headers needed by this file */
#include "H5VLpublic.h"        /* Virtual Object Layer                 */

/* Identifier for the pass-through VOL connector */
#define H5VL_PASSTHRU_EXT	(H5VL_pass_through_ext_register())

/* Characteristics of the pass-through VOL connector */
#define H5VL_PASSTHRU_EXT_NAME        "pass_through_ext"
#define H5VL_PASSTHRU_EXT_VALUE       517           /* VOL connector ID */
#define H5VL_PASSTHRU_EXT_VERSION     0
/* Pass-through VOL connector info */
typedef struct H5VL_pass_through_ext_info_t {
    hid_t under_vol_id;         /* VOL ID for under VOL */
    void *under_vol_info;       /* VOL info for under VOL */
} H5VL_pass_through_ext_info_t;


#ifdef __cplusplus
extern "C" {
#endif

/* New "public" API routines */
herr_t H5Dfoo(hid_t dset_id, hid_t dxpl_id, void **req, int i, double d);
herr_t H5Dbar(hid_t dset_id, hid_t dxpl_id, void **req, double *dp, unsigned *up);
herr_t H5Gfiddle(hid_t group_id, hid_t dxpl_id, void **req);
  herr_t H5Freserve_cache(hid_t file_id, hid_t hid_dxpl_id, void **req, hsize_t size);
  herr_t H5Fquery_cache(hid_t file_id, hid_t hid_dxpl_id, void **req, hsize_t *size);
H5_DLL hid_t H5VL_pass_through_ext_register(void);

#ifdef __cplusplus
}
#endif

#endif /* _H5VLpassthru_H */

