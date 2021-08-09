/*
 * Purpose:	The public header file for the caching VOL connector.
 */

#ifndef _H5VLcache_ext_H
#define _H5VLcache_ext_H

#include "mpi.h"
#include "cache_utils.h"

/* Public headers needed by this file */
#include "H5VLpublic.h"        /* Virtual Object Layer                 */
#include "H5LS.h"

/* Identifier for the CACHE VOL connector */
#define H5VL_CACHE_EXT	(H5VL_cache_ext_register())

/* Characteristics of the CACHE VOL connector */
#define H5VL_CACHE_EXT_NAME        "cache_ext"
#define H5VL_CACHE_EXT_VALUE       513           /* VOL connector ID */

/* The Cache VOL connector info */
typedef struct H5VL_cache_ext_info_t {
    hid_t under_vol_id;         /* VOL ID for under VOL */
    void *under_vol_info;       /* VOL info for under VOL */
    char fconfig[255];          /* file name for config, this is specific to caching VOL */
} H5VL_cache_ext_info_t;


/* The Cache VOL info object */
typedef struct H5VL_cache_ext_t {
    hid_t  under_vol_id;        /* ID for underlying VOL connector */
    void  *under_object;       /* Info object for underlying VOL connector */
    // the following are specific to caching vol. 
    io_handler_t *H5DRMM; // for read
    io_handler_t *H5DWMM; // for write
    bool async_pause; 
    bool read_cache;
    bool write_cache;
    bool read_cache_info_set; 
    bool write_cache_info_set; 
    int num_request_dataset;
    void *prefetch_req;
    hid_t hd_glob;
    hid_t es_id; // event set id associated to all 
    void *parent; // parent object, file->group->dataset
    cache_storage_t *H5LS;
} H5VL_cache_ext_t;


#ifdef __cplusplus
extern "C" {
#endif

H5_DLL hid_t H5VL_cache_ext_register(void);

#ifdef __cplusplus
}
#endif


#endif /* _H5VLcache_H */

