from spack import *

class Hdf5volcache(CMakePackage):
    """ HDF5 Cache VOL: Efficient Parallel I/O throught Caching data on Node Local Storage."""

    homepage = "https://vol-cache.readthedocs.io"
    git      = "https://github.com/hpc-io/vol-cache"

    maintainers = ['zhenghh04']

    version('develop', branch='develop')
    version('1.2', tag='v1.2')
    version('1.1', tag='v1.1')
    version('1.0', tag='v1.0')

    depends_on('argobots@main')
    depends_on['mpi']
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
