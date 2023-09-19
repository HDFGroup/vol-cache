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
#include <string.h>
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

int LOG_INIT(int rank) {
  HDF5_CACHE_RANK_ID = rank;
  HDF5_CACHE_LOG_LEVEL = log_level();
  HDF5_CACHE_IO_NODE = io_node();
  return 0;
}

void LOG_INFO(const char *app_file, const char *app_func, unsigned app_line,
              int rank, const char *str) {
#ifndef NDEBUG
  if (HDF5_CACHE_LOG_LEVEL >= INFO)
    if (rank >= 0)
      printf(" [CACHE VOL][INFO][%d] %s: %s \t <%s:%s:L%d>\n", rank, GET_TIME(),
             str, app_file, app_func, app_line);
    else if (HDF5_CACHE_RANK_ID == HDF5_CACHE_IO_NODE)
      printf(" [CACHE VOL][INFO] %s: %s \t <%s:%s:L%d>\n", GET_TIME(), str,
             app_file, app_func, app_line);
#endif
}

void LOG_ERROR(const char *app_file, const char *app_func, unsigned app_line,
               int rank, const char *str) {
#ifndef NDEBUG
  if (HDF5_CACHE_LOG_LEVEL >= ERROR)
    if (rank >= 0)
      printf(" [CACHE VOL][ERROR][%d] %s: %s \t  <%s:%s:L%d>\n", rank,
             GET_TIME(), str, app_file, app_func, app_line);
    else if (HDF5_CACHE_RANK_ID == HDF5_CACHE_IO_NODE)
      printf(" [CACHE VOL][ERROR] %s: %s \t  <%s:%s:L%d>\n", GET_TIME(), str,
             app_file, app_func, app_line);
#endif
}

void LOG_DEBUG(const char *app_file, const char *app_func, unsigned app_line,
               int rank, const char *str) {
#ifndef NDEBUG
  if (HDF5_CACHE_LOG_LEVEL >= DEBUG)
    if (rank >= 0)
      printf(" [CACHE VOL][DEBUG][%d] %s: %s \t  <%s:%s:L%d>\n", rank,
             GET_TIME(), str, app_file, app_func, app_line);
    else if (HDF5_CACHE_RANK_ID == HDF5_CACHE_IO_NODE)
      printf(" [CACHE VOL][DEBUG] %s: %s \t <%s:%s:L%d>\n", GET_TIME(), str,
             app_file, app_func, app_line);
#endif
}

void LOG_WARN(const char *app_file, const char *app_func, unsigned app_line,
              int rank, const char *str) {
#ifndef NDEBUG
  if (HDF5_CACHE_LOG_LEVEL >= WARN)
    if (rank >= 0)
      printf(" [CACHE VOL][WARN][%d] %s:  %s \t <%s:%s:L%d>\n", rank,
             GET_TIME(), str, app_file, app_func, app_line);
    else if (HDF5_CACHE_RANK_ID == HDF5_CACHE_IO_NODE)
      printf(" [CACHE VOL][WARN] %s: %s \t <%s:%s:L%d>\n", GET_TIME(), str,
             app_file, app_func, app_line);
#endif
}

void LOG_TRACE(const char *app_file, const char *app_func, unsigned app_line,
               int rank, const char *str) {
#ifndef NDEBUG
  struct timeval now;
  gettimeofday(&now, NULL);
  if (HDF5_CACHE_LOG_LEVEL >= TRACE)
    if (rank >= 0)
      printf(" [CACHE VOL][TRACE] [%d] %s: %s \t <%s:%s:L%d>\n", rank,
             GET_TIME(), str, app_file, app_func, app_line);
    else if (HDF5_CACHE_RANK_ID == HDF5_CACHE_IO_NODE)
      printf(" [CACHE VOL][TRACE] %s: %s \t <%s:%s:L%d>\n", GET_TIME(), str,
             app_file, app_func, app_line);
#endif
}
