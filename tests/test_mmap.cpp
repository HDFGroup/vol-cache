/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (c) 2023, UChicago Argonne, LLC.                                *
 * All Rights Reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5 Cache VOL connector.  The full copyright notice *
 * terms governing use, modification, and redistribution, is contained in    *
 * the LICENSE file, which can be found at the root of the source code       *
 * distribution tree.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
   This is one of the unit test in Cache VOL. The goal of this test is to find
   out whether the system support mmap or not.
 */
#include "fcntl.h"
#include "mpi.h"
#include "string.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>
void int2char(int a, char str[255]) { sprintf(str, "%d", a); }
#define PBSTR "============================================================\n"
#define PBWIDTH 60

void printProgress(double percentage, const char *pre = NULL) {
  int val = (int)(percentage * 100);
  int lpad = (int)(percentage * PBWIDTH);
  int rpad = PBWIDTH - lpad;
  if (pre != NULL)
    printf("\r%s %3d%% [%.*s>%*s]", pre, val, lpad, PBSTR, rpad, "");
  else
    printf("\r%3d%% [%.*s>%*s]", val, lpad, PBSTR, rpad, "");
  fflush(stdout);
}

int remote_put(void *buf, int count, int src, int dist, void *mmap_p,
               MPI_Aint disp, MPI_Comm comm) {
  int rank;
  MPI_Comm_rank(comm, &rank);
  int tag = rank;
  char *p = (char *)mmap_p;
  MPI_Request request;
  MPI_Status status;

  if (src == dist) {
    if (rank == src)
      memcpy(&p[disp], buf, count);
  } else {
    if (rank == dist) {
      printf(" [%d] received and write to the drive\n", rank);
      MPI_Irecv(&p[disp], count, MPI_CHAR, src, tag, comm, &request);
    } else if (rank == src) {
      printf("[%d] rank %d send data to %d \n", rank, src, dist);
      MPI_Send(buf, count, MPI_CHAR, dist, tag, comm);
    }
    if (rank == dist)
      MPI_Wait(&request, &status);
  }
  return 0;
}

int remote_get(void *buf, int count, int rank, int src, int dist, void *mmap_p,
               MPI_Aint disp, MPI_Comm comm) {
  int tag = 0;
  char *p = (char *)mmap_p;
  if (src == dist) {
    if (rank == src)
      memcpy(buf, &p[disp], count);
  } else {
    if (rank == dist)
      MPI_Recv(buf, count, MPI_CHAR, src, tag, comm, MPI_STATUS_IGNORE);
    else if (rank == src) {
      char *p = (char *)mmap_p;
      MPI_Send(&p[disp], count, MPI_CHAR, dist, tag, comm);
    }
  }
  return 0;
}

void create_read_mmap(char *name, long unsigned int size,
                      long unsigned int offset, int verbose = 0) {
  int fh = open(name, O_RDWR | O_CREAT | O_TRUNC,
                S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  MPI_Info info;
  int rank, nproc;
  MPI_Comm_size(MPI_COMM_WORLD, &nproc);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  char p[1024];
  for (int i = 0; i < 1024; i++)
    p[i] = 'a';
  int kb = 1024;
  MPI_Aint disp;
  if (getenv("MPI_WIN")) {
    MPI_Win_create(buf, size, 1, MPI_INFO_NULL, MPI_COMM_WORLD, &win);
    MPI_Win_fence(MPI_MODE_NOPRECEDE, win);
    for (long unsigned int i = 0; i < size / kb; i++) {
      disp = i;
      disp = disp * kb;
      if (verbose == 1 and i % 1024 == 1023)
        printProgress(float(i + 1) * kb / size);
      MPI_Put(&p[0], kb, MPI_CHAR, (rank + 1) % nproc, disp, kb, MPI_CHAR, win);
    }
    MPI_Win_fence(MPI_MODE_NOSUCCEED, win);
    MPI_Win_free(&win);
  } else {
    for (long unsigned int i = 0; i < size / kb; i++) {
      disp = i;
      disp = disp * kb;
      if (verbose == 1 and i % 1024 == 1023)
        printProgress(float(i + 1) * kb / size);
      remote_put(&p[0], kb, rank, (rank + 1) % nproc, buf, disp,
                 MPI_COMM_WORLD);
    }
    MPI_Info info;
    MPI_Win_create(buf, size, 1, MPI_INFO_NULL, MPI_COMM_WORLD, &win);
    int rank, nproc;
    MPI_Comm_size(MPI_COMM_WORLD, &nproc);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Win_fence(MPI_MODE_NOPRECEDE, win);
    char p[1024];
    for (int i = 0; i < 1024; i++)
      p[i] = 'a';
    int kb = 1024;
    MPI_Aint disp;
    for (long unsigned int i = 0; i < size / kb; i++) {
      disp = i;
      disp = disp * kb;
      if (verbose == 1 and i % 1024 == 1023)
        printProgress(float(i + 1) * kb / size);
      MPI_Put(&p[0], kb, MPI_CHAR, rank, disp, kb, MPI_CHAR, win);
    }
    munmap(buf, size);
    close(fh);
  }

  int main(int argc, char *argv[]) {
    MPI_Init(&argc, &argv);
    int nproc, rank;
    MPI_Comm_size(MPI_COMM_WORLD, &nproc);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    size_t GB = 1024 * 1024 * 1024;
    int s = int(atof(argv[1]));
    char crank[255];
    int2char(rank, crank);
    long unsigned int size = s;
    size = size * GB;
    long unsigned int offset = 0;
    char name[255];
    strcpy(name, "mmap-");
    strcat(name, crank);
    strcat(name, ".dat");
    if (rank == 0) {
      printf("Number of processes: %d\n", nproc);
      printf("Total size of the mmap: %d GB\n", s * nproc);
    }
    int verbose = 0;
    if (rank == 0)
      verbose = 1;
    create_read_mmap(name, size, offset, verbose);
    MPI_Finalize();
    return 0;
  }
