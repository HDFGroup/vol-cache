#ifndef _new_h5api_H
#define _new_h5api_H
#include "hdf5.h"
#include "H5VLcache_ext.h"
#define H5VL_CACHE_EXT_DYN_DFOO "anl.gov.cache.dfoo"
#define H5VL_CACHE_EXT_DYN_DREAD_TO_CACHE "anl.gov.cache.dread_to_cache"
#define H5VL_CACHE_EXT_DYN_DPREFETCH "anl.gov.cache.dprefetch"
#define H5VL_CACHE_EXT_DYN_DREAD_FROM_CACHE "anl.gov.cache.dread_from_cache"
#define H5VL_CACHE_EXT_DYN_DCACHE_REMOVE "anl.gov.cache.dcache_remove"
#define H5VL_CACHE_EXT_DYN_DCACHE_CREATE "anl.gov.cache.dcache_create"
#define H5VL_CACHE_EXT_DYN_DMMAP_REMAP "anl.gov.cache.dmmap_remap"
#define H5VL_CACHE_EXT_DYN_DBAR "anl.gov.cache.dbar"
#define H5VL_CACHE_EXT_DYN_GFIDDLE "anl.gov.cache.gfiddle"
#define H5VL_CACHE_EXT_DYN_FCACHE_REMOVE "anl.gov.fcache.remove"
#define H5VL_CACHE_EXT_DYN_FCACHE_CREATE "anl.gov.fcache.create"
#endif


#ifdef __cplusplus
extern "C" {
#endif
/* New "public" API routines */
  herr_t H5Dmmap_remap(hid_t dset_id);
  herr_t H5Dfoo(hid_t dset_id, hid_t dxpl_id, int i, double d);
  herr_t H5Dprefetch(hid_t dset_id, hid_t file_space_id, hid_t dxpl_id);
  herr_t H5Dread_to_cache(hid_t dset_id, hid_t mem_type_id, hid_t memspace_id, hid_t file_space_id, hid_t dxpl_id, void *buf);
  herr_t H5Dread_from_cache(hid_t dset_id, hid_t mem_type_id, hid_t memspace_id, hid_t file_space_id, hid_t dxpl_id, void *buf);
  herr_t H5Dbar(hid_t dset_id, hid_t dxpl_id, double *dp, unsigned *up);
  herr_t H5Gfiddle(hid_t group_id, hid_t dxpl_id);
  herr_t H5Dmmap_remap(hid_t group_id);
  herr_t H5Freserve_cache(hid_t file_id, hid_t hid_dxpl_id, hsize_t size, cache_purpose_t purpose, cache_duration_t duration);
  herr_t H5Fquery_cache(hid_t file_id, hid_t hid_dxpl_id, hsize_t *size);
  herr_t H5Fcache_create(hid_t file_id, hid_t dapl_id, hsize_t size, cache_purpose_t purpose, cache_duration_t duration);
  herr_t H5Fcache_remove(hid_t file_id);
  herr_t H5Dcache_remove(hid_t dset_id);
  herr_t H5Dcache_create(hid_t dset_id, char *name);
  H5_DLL hid_t H5VL_cache_ext_register(void);
#ifdef __cplusplus
}
#endif
