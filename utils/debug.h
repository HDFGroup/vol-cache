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
#include "stdio.h"
#include "stdlib.h"
#include <stddef.h>
#ifdef __cplusplus
extern "C" {
#endif
int io_node();
int log_level();
extern int HDF5_CACHE_RANK_ID;
extern int HDF5_CACHE_IO_NODE;
extern int HDF5_CACHE_LOG_LEVEL;
void log_trace(const char *app_file, const char *app_func, unsigned app_line,
               int rank, const char *str);
void log_debug(const char *app_file, const char *app_func, unsigned app_line,
               int rank, const char *str);
void log_info(const char *app_file, const char *app_func, unsigned app_line,
              int rank, const char *str);
void log_error(const char *app_file, const char *app_func, unsigned app_line,
               int rank, const char *str);
void log_warn(const char *app_file, const char *app_func, unsigned app_line,
              int rank, const char *str);
void *my_malloc(const char *file, int line, const char *func, size_t size);
void my_free(const char *file, int line, const char *func, void *p);
void *my_calloc(const char *file, int line, const char *func, int count,
                size_t size);
void log_init(int rank);                
#ifndef LOG_IMPL
#define LOG_IMPL
#define LOG_INIT(X) log_init(X)
#define LOG_DEBUG(X, ...)                                                 \
    {                                                                     \
      char msg_debug[283];                                                \
      sprintf(msg_debug, __VA_ARGS__);                                    \
      log_debug(__FILE__, __func__, __LINE__, X, msg_debug);              \
    }
#define LOG_WARN(X, ...)                                                  \
    {                                                                     \
      char msg_debug[283];                                                \
      sprintf(msg_debug, __VA_ARGS__);                                    \
      log_warn(__FILE__, __func__, __LINE__, X, msg_debug);               \
    } 
#define LOG_INFO(X, ...)                                                  \
    {                                                                     \
      char msg_debug[283];                                                \
      sprintf(msg_debug, __VA_ARGS__);                                    \
      log_info(__FILE__, __func__, __LINE__, X, msg_debug);               \
    }   
#define LOG_ERROR(X, ...)                                                 \
    {                                                                     \
      char msg_debug[283];                                                \
      sprintf(msg_debug, __VA_ARGS__);                                    \
      log_error(__FILE__, __func__, __LINE__, X, msg_debug);              \
    }        
#define LOG_TRACE(X, ...)                                                 \
    {                                                                     \
      char msg_debug[283];                                                \
      sprintf(msg_debug, __VA_ARGS__);                                    \
      log_trace(__FILE__, __func__, __LINE__, X, msg_debug);              \
    }           
//#define LOG_DEBUG(...) log_debug(__FILE__, __func__, __LINE__, __VA_ARGS__)
//#define LOG_ERROR(...) log_error(__FILE__, __func__, __LINE__, __VA_ARGS__)
//#define LOG_WARN(...) log_warn(__FILE__, __func__, __LINE__, __VA_ARGS__)
//#define LOG_INFO(...) log_info(__FILE__, __func__, __LINE__, __VA_ARGS__)
#define malloc(...) my_malloc(__FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define free(...) my_free(__FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define calloc(...) my_calloc(__FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#endif
#ifdef __cplusplus
}
#endif
#endif
