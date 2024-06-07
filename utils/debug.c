/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (c) 2023, UChicago Argonne, LLC.                                *
 * All Rights Reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5 Cache VOL connector.  The full copyright notice *
 * terms governing use, modification, and redistribution, is contained in    *
 * the LICENSE file, which can be found at the root of the source code       *
 * distribution tree.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#include "stdio.h"
#include "stdlib.h"
#include <stddef.h>
#include <string.h>
#include <sys/time.h>
#ifndef STDERR
#ifdef __APPLE__
#define STDERR __stderrp
#else
#define STDERR stderr
#endif
#endif

//#include "debug.h"
int HDF5_CACHE_RANK_ID;
int HDF5_CACHE_IO_NODE;
int HDF5_CACHE_LOG_LEVEL;
enum log_level_t { ERROR, WARN, INFO, DEBUG, TRACE };
int io_node() {
  if (getenv("HDF5_CACHE_IO_NODE") != NULL)
    return atof(getenv("HDF5_CACHE_IO_NODE"));
  else
    return 0;
}

int log_level() {
  if (getenv("HDF5_CACHE_LOG_LEVEL") != NULL) {
    char *log = getenv("HDF5_CACHE_LOG_LEVEL");
    if (!strcmp(log, "DEBUG") || !strcmp(log, "debug")) {
      return DEBUG;
    } else if (!strcmp(log, "WARN") || !strcmp(log, "warn")) {
      return WARN;
    } else if (!strcmp(log, "INFO") || !strcmp(log, "info")) {
      return INFO;
    } else if (!strcmp(log, "TRACE") || !strcmp(log, "trace")) {
      return TRACE;
    } else {
      return ERROR;
    }
  } else
    return ERROR;
}
char *GET_TIME() {
  char *str;
  str = (char *)malloc(255);
  struct timeval now;
  gettimeofday(&now, NULL);
  sprintf(str, "%ld.%06ld", now.tv_sec, now.tv_usec);
  return str;
}

int log_init(int rank) {
  HDF5_CACHE_RANK_ID = rank;
  HDF5_CACHE_LOG_LEVEL = log_level();
  HDF5_CACHE_IO_NODE = io_node();
  return 0;
}

void log_info(const char *app_file, const char *app_func, unsigned app_line,
              int rank, const char *str) {
#ifndef NDEBUG
  if (HDF5_CACHE_LOG_LEVEL >= INFO)
    if (rank >= 0)
      printf(" [CACHE VOL][INFO][%d] %s: %s \n\t <%s:%d:%s>\n", rank,
             GET_TIME(), str, app_file, app_line, app_func);
    else if (HDF5_CACHE_RANK_ID == HDF5_CACHE_IO_NODE)
      printf(" [CACHE VOL][INFO] %s: %s \n\t <%s:%d:%s>\n", GET_TIME(), str,
             app_file, app_line, app_func);
#endif
}

void log_error(const char *app_file, const char *app_func, unsigned app_line,
               int rank, const char *str) {
  if (HDF5_CACHE_LOG_LEVEL >= ERROR)
    if (rank >= 0) {
      printf(" [CACHE VOL][ERROR][%d] %s: %s \n\t <%s:%d:%s>\n", rank,
             GET_TIME(), str, app_file, app_line, app_func);
      fprintf(STDERR, " [CACHE VOL][ERROR][%d] %s: %s \n\t <%s:%d:%s>\n", rank,
              GET_TIME(), str, app_file, app_line, app_func);
    } else if (HDF5_CACHE_RANK_ID == HDF5_CACHE_IO_NODE) {
      printf(" [CACHE VOL][ERROR] %s: %s \n\t <%s:%d:%s>\n", GET_TIME(), str,
             app_file, app_line, app_func);
      fprintf(STDERR, " [CACHE VOL][ERROR] %s: %s \n\t <%s:%d:%s>\n",
              GET_TIME(), str, app_file, app_line, app_func);
    }
}

void log_debug(const char *app_file, const char *app_func, unsigned app_line,
               int rank, const char *str) {
#ifndef NDEBUG
  if (HDF5_CACHE_LOG_LEVEL >= DEBUG)
    if (rank >= 0)
      printf(" [CACHE VOL][DEBUG][%d] %s: %s \n\t <%s:%d:%s>\n", rank,
             GET_TIME(), str, app_file, app_line, app_func);
    else if (HDF5_CACHE_RANK_ID == HDF5_CACHE_IO_NODE)
      printf(" [CACHE VOL][DEBUG] %s: %s \n\t <%s:%d:%s>\n", GET_TIME(), str,
             app_file, app_line, app_func);
#endif
}

void log_warn(const char *app_file, const char *app_func, unsigned app_line,
              int rank, const char *str) {
#ifndef NDEBUG
  if (HDF5_CACHE_LOG_LEVEL >= WARN)
    if (rank >= 0)
      printf(" [CACHE VOL][WARN][%d] %s:  %s \n\t <%s:%d:%s>\n", rank,
             GET_TIME(), str, app_file, app_line, app_func);
    else if (HDF5_CACHE_RANK_ID == HDF5_CACHE_IO_NODE)
      printf(" [CACHE VOL][WARN] %s: %s \n\t <%s:%d:%s>\n", GET_TIME(), str,
             app_file, app_line, app_func);
#endif
}

void log_trace(const char *app_file, const char *app_func, unsigned app_line,
               int rank, const char *str) {
#ifndef NDEBUG
  struct timeval now;
  gettimeofday(&now, NULL);
  if (HDF5_CACHE_LOG_LEVEL >= TRACE)
    if (rank >= 0)
      printf(" [CACHE VOL][TRACE] [%d] %s: %s \n\t <%s:%d:%s>\n", rank,
             GET_TIME(), str, app_file, app_line, app_func);
    else if (HDF5_CACHE_RANK_ID == HDF5_CACHE_IO_NODE)
      printf(" [CACHE VOL][TRACE] %s: %s \n\t <%s:%d:%s>\n", GET_TIME(), str,
             app_file, app_line, app_func);
#endif
}

void *my_malloc(const char *file, int line, const char *func, size_t size) {
  void *p = malloc(size);
#ifndef NDEBUG
  if (HDF5_CACHE_LOG_LEVEL >= DEBUG)
    printf(" [CACHE VOL][DEBUG] MEMORY Allocated \n\t <%s:%i:%s>:  %p[%li]\n",
           file, line, func, p, size);
#endif
  return p;
}

void *my_calloc(const char *file, int line, const char *func, int count,
                size_t size) {
  void *p = calloc(count, size);
#ifndef NDEBUG
  if (HDF5_CACHE_LOG_LEVEL >= DEBUG)
    printf(" [CACHE VOL][DEBUG] MEMORY Allocated \n\t <%s:%i:%s>: %p[%dx%li]\n",
           file, line, func, p, count, size);
#endif
  return p;
}

void my_free(const char *file, int line, const char *func, void *p) {
#ifndef NDEBUG
  if (HDF5_CACHE_LOG_LEVEL >= DEBUG)
    printf(" [CACHE VOL][DEBUG] MEMORY Deallocated \n\t <%s:%i:%s>: %p\n", file,
           line, func, p);
#endif
  free(p);
}
