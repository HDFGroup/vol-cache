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
#include "mpi.h"
#include "cache_utils.h"
/* Public headers needed by this file */
#include "H5VLpublic.h"        /* Virtual Object Layer                 */


/* Identifier for the CACHE VOL connector */
#define H5VL_CACHE_EXT	(H5VL_cache_ext_register())

/* Characteristics of the CACHE VOL connector */
#define H5VL_CACHE_EXT_NAME        "cache_ext"
#define H5VL_CACHE_EXT_VALUE       518           /* VOL connector ID */
#define H5VL_CACHE_EXT_VERSION     0


#define H5P_LOCAL_STORAGE_CREATE (H5OPEN H5P_CLS_LOCAL_STORAGE_CREATE_ID_g)
H5_DLLVAR hid_t H5P_CLS_LOCAL_STORAGE_CREATE_ID_g; 


#endif /* _H5VLcache_H */

