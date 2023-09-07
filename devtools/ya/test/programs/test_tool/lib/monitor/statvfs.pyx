# os.statvfs doesn't provide f_fsid, see bugs.python.org/issue32143

ctypedef unsigned long ulong

cdef extern from "devtools/ya/test/programs/test_tool/lib/monitor/statvfs.h" nogil:
    cdef struct TStatVfs:
        ulong f_bsize;
        ulong f_frsize;
        ulong f_blocks;
        ulong f_bfree;
        ulong f_bavail;
        ulong f_files;
        ulong f_ffree;
        ulong f_favail;
        ulong f_fsid;
        ulong f_flag;
        ulong f_namemax;

    int get_statvfs(const char *path, TStatVfs& st)

from cpython.exc cimport PyErr_SetFromErrno


def statvfs(path):
    cdef TStatVfs st
    cdef const char* path_ptr = path
    cdef int ret = get_statvfs(path_ptr, st)
    if ret != 0:
        PyErr_SetFromErrno(OSError)
    return st
