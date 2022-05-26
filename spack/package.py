from spack import *

class Hdf5volcache(CMakePackage):
    """ HDF5 Cache VOL: Efficient Parallel I/O throught Caching data on Node Local Storage."""

    homepage = "https://vol-cache.readthedocs.io"
    git      = "https://github.com/hpc-io/vol-cache"

    maintainers = ['huihuo']

    version('develop', branch='develop')

    depends_on('argobots@main')
    depends_on('hdf5@develop-1.13+mpi+threadsafe')
    depends_on('hdf5-async-vol@develop')

    def cmake_args(self):
        """Populate cmake arguments for HDF5 VOL."""
        spec = self.spec

        args = [
            '-DBUILD_SHARED_LIBS:BOOL=ON',
            '-DBUILD_TESTING:BOOL=ON',
            '-DHDF5_ENABLE_PARALLEL:BOOL=ON',
            '-DHDF5_ENABLE_THREADSAFE:BOOL=ON',
            '-DALLOW_UNSUPPORTED:BOOL=ON',
        ]
        return args
