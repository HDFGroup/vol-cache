/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (c) 2023, UChicago Argonne, LLC.                                *
 * All Rights Reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5 Cache VOL connector.  The full copyright notice *
 * terms governing use, modification, and redistribution, is contained in    *
 * the LICENSE file, which can be found at the root of the source code       *
 * distribution tree.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#ifndef DEBUG_H__
#define DEBUG_H__
#ifdef __cplusplus
extern "C" {
#endif
int io_node();
int log_level();
extern int HDF5_CACHE_RANK_ID;
extern int HDF5_CACHE_IO_NODE;
extern int HDF5_CACHE_LOG_LEVEL;
void LOG_TRACE(const char *app_file, const char *app_func, unsigned app_line,
               int rank, const char *str);
void LOG_DEBUG(const char *app_file, const char *app_func, unsigned app_line,
               int rank, const char *str);
void LOG_INFO(const char *app_file, const char *app_func, unsigned app_line,
              int rank, const char *str);
void LOG_ERROR(const char *app_file, const char *app_func, unsigned app_line,
               int rank, const char *str);
void LOG_WARN(const char *app_file, const char *app_func, unsigned app_line,
              int rank, const char *str);
#ifndef LOG_IMPL
#define LOG_IMPL
#define LOG_TRACE(...)                                                         \
  LOG_TRACE(basename(__FILE__), __func__, __LINE__, __VA_ARGS__)
#define LOG_DEBUG(...)                                                         \
  LOG_DEBUG(basename(__FILE__), __func__, __LINE__, __VA_ARGS__)
#define LOG_ERROR(...)                                                         \
  LOG_ERROR(basename(__FILE__), __func__, __LINE__, __VA_ARGS__)
#define LOG_WARN(...)                                                          \
  LOG_WARN(basename(__FILE__), __func__, __LINE__, __VA_ARGS__)
#define LOG_INFO(...)                                                          \
  LOG_INFO(basename(__FILE__), __func__, __LINE__, __VA_ARGS__)
#endif
#ifdef __cplusplus
}
#endif
#endif
