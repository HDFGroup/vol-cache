/*
 * Purpose:	The private header file for the caching VOL connector.
 */

#ifndef _H5VLcache_ext_private_H
#define _H5VLcache_ext_private_H

#include "mpi.h"
#include "cache_utils.h"

/* Public headers needed by this file */
#include "H5VLpublic.h"        /* Virtual Object Layer                 */
#include "H5LS.h"

/* Public headers needed by this file */
#include "H5VLcache_ext.h"        /* Public header for connector */

/* Characteristics of the CACHE VOL connector */
#define H5VL_CACHE_EXT_VERSION     0

/* Names for dynamically registered operations */
#define H5VL_CACHE_EXT_DYN_DREAD_TO_CACHE "anl.gov.cache.dread_to_cache"
#define H5VL_CACHE_EXT_DYN_DPREFETCH "anl.gov.cache.dprefetch"
#define H5VL_CACHE_EXT_DYN_DREAD_FROM_CACHE "anl.gov.cache.dread_from_cache"
#define H5VL_CACHE_EXT_DYN_DCACHE_REMOVE "anl.gov.cache.dcache_remove"
#define H5VL_CACHE_EXT_DYN_DCACHE_CREATE "anl.gov.cache.dcache_create"
#define H5VL_CACHE_EXT_DYN_DMMAP_REMAP "anl.gov.cache.dmmap_remap"
#define H5VL_CACHE_EXT_DYN_FCACHE_REMOVE "anl.gov.fcache.remove"
#define H5VL_CACHE_EXT_DYN_FCACHE_CREATE "anl.gov.fcache.create"
#define H5VL_CACHE_EXT_DYN_FCACHE_ASYNC_OP_PAUSE "anl.gov.fcache.async.op.pause"
#define H5VL_CACHE_EXT_DYN_FCACHE_ASYNC_OP_START "anl.gov.fcache.async.op.start"

#define H5VL_CACHE_EXT_DYN_DCACHE_ASYNC_OP_PAUSE "anl.gov.dcache.async.op.pause"
#define H5VL_CACHE_EXT_DYN_DCACHE_ASYNC_OP_START "anl.gov.dcache.async.op.start"


/* Parameters for each of the dynamically registered operations */

/* H5VL_CACHE_EXT_DYN_DREAD_TO_CACHE */
typedef struct H5VL_cache_ext_dataset_read_to_cache_args_t {
    hid_t mem_type_id;
    hid_t mem_space_id;
    hid_t file_space_id;
    void *buf;
} H5VL_cache_ext_dataset_read_to_cache_args_t;

/* H5VL_CACHE_EXT_DYN_DPREFETCH */
typedef struct H5VL_cache_ext_dataset_prefetch_args_t {
    hid_t file_space_id;
} H5VL_cache_ext_dataset_prefetch_args_t;

/* H5VL_CACHE_EXT_DYN_DREAD_FROM_CACHE */
typedef struct H5VL_cache_ext_dataset_read_from_cache_args_t {
    hid_t mem_type_id;
    hid_t mem_space_id;
    hid_t file_space_id;
    void *buf;
} H5VL_cache_ext_dataset_read_from_cache_args_t;

/* H5VL_CACHE_EXT_DYN_DCACHE_CREATE */
typedef struct H5VL_cache_ext_dataset_cache_create_args_t {
    const char *name;
} H5VL_cache_ext_dataset_cache_create_args_t;

/* H5VL_CACHE_EXT_DYN_FCACHE_CREATE */
typedef struct H5VL_cache_ext_file_cache_create_args_t {
    hid_t fapl_id;
    hsize_t size;
    cache_purpose_t purpose;
    cache_duration_t duration;
} H5VL_cache_ext_file_cache_create_args_t;

#ifdef __cplusplus
extern "C" {
#endif

#ifdef __cplusplus
}
#endif

#endif /* _H5VLcache_ext_private_H */

