"""
I was using this to test the performance of NBD virtual block devices by
creating a loopback to a physical disk.

It's difficult to impossible to use direct i/o from Python without bindings.
"""

import os

cdef extern from 'stdlib.h':
    int posix_memalign(char **memptr, size_t alignment, size_t size)
    void free(char *)
    int errno

cdef extern from 'unistd.h':
    unsigned long pread(int, char *, unsigned long, unsigned long)
    unsigned long pwrite(int, char *, unsigned long, unsigned long)

cdef extern from 'string.h':
    void *memcpy(char *, char *, int)

cdef class DirectFile:
    cdef int fd, align

    def __init__(self, path, align=4096):
        self.fd = os.open(path, os.O_DIRECT | os.O_RDWR)
        self.align = align

    def __dealloc__(self):
        os.close(self.fd)

    def pread(self, offset, len):
        cdef char *buf
        posix_memalign(&buf, self.align, len)
        read_amt = pread(self.fd, buf, len, offset)
        if read_amt < 0:
            raise OSError(errno, 'Error Reading')
        resp = buf[0:read_amt]
        free(buf)
        return resp

    def pwrite(self, offset, buf):
        cdef char *buf2
        posix_memalign(&buf2, self.align, len(buf))
        memcpy(buf2, buf, len(buf))
        write_amt = pwrite(self.fd, buf2, len(buf), offset)
        if write_amt < 0:
            raise OSError(errno, 'Error Writing')
        free(buf2)

    def __len__(self):
        return os.lseek(self.fd, 0, os.SEEK_END)
