#include "fcntl.h"
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include "mpi.h"
#include "string.h"
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

void create_read_mmap(char *name, long unsigned int size, long unsigned int offset, int verbose=0) {
  int fh = open(name, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  char a = 'A';
  pwrite(fh, &a, 1, offset+size);
  close(fh);
  fh = open(name, O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  void *buf = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fh, offset);
  MPI_Win win;
  MPI_Info info; 
  MPI_Win_create(buf, size, 1, MPI_INFO_NULL, MPI_COMM_WORLD, &win);
  int rank, nproc; 
  MPI_Comm_size(MPI_COMM_WORLD, &nproc);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank); 
  MPI_Win_fence(MPI_MODE_NOPRECEDE, win);
  char p[1024];
  for (int i=0; i<1024; i++) p[i]='a';
  int kb=1024;
  MPI_Aint disp; 
  for(long unsigned int i=0; i<size/kb; i++) {
    disp = i;
    disp = disp*kb; 
    if (verbose==1 and i%1024==1023) printProgress(float(i+1)*kb/size);
    MPI_Put(&p[0], kb, MPI_CHAR, rank, disp, kb, MPI_CHAR, win); 
  }
  MPI_Win_fence(MPI_MODE_NOSUCCEED, win);
  MPI_Win_free(&win);
  munmap(buf, size);
  close(fh);
}


int main(int argc, char *argv[]) {
  MPI_Init(&argc, &argv);
  int nproc, rank; 
  MPI_Comm_size(MPI_COMM_WORLD, &nproc);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank); 
  size_t GB=1024*1024*1024;
  int s = int(atof(argv[1]));
  char crank[255];
  int2char(rank, crank);
  long unsigned int size = s;
  size = size*GB;
  long unsigned int offset = 0;
  char name[255];
  strcpy(name, "mmap-");
  strcat(name, crank);
  strcat(name, ".dat");
  if (rank==0) {
    printf("Number of processes: %d\n", nproc);
    printf("Total size of the mmap: %d GB\n", s*nproc);
  }
  int verbose = 0;
  if (rank==0) verbose=1;
  create_read_mmap(name, size, offset, verbose);
  MPI_Finalize();
  return 0;
}
