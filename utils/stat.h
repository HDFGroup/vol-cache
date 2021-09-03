/*
  This is to compute a mean and standard deviation of
  an array or an inverse of the array, depending
  on the setting:
    'n' -- average over x;
    'i' -- average over 1/x;

  Huihuo Zheng @ Argonne Leadership Computing Facility
 */
#ifndef STAT_H_
#define STAT_H_
#include <cmath>
#include <stdio.h>
template <typename T>
void stat(T *array, int n, T &avg, T &std, char type = 'n') {
  T x, xx;
  x = 0;
  xx = 0;
  if (type == 'n') {
    for (int i = 0; i < n; i++) {
      x += array[i];
      xx += array[i] * array[i];
    }
  } else {
    for (int i = 0; i < n; i++) {
      x += 1.0 / array[i];
      xx += 1.0 / array[i] / array[i];
    }
  }
  avg = x / n;
  std = sqrt(xx / n - avg * avg);
}

#endif
