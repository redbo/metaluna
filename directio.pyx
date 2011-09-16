"""
I was using this to test the performance of NBD virtual block devices by
creating a loopback to a physical disk.

It's difficult to impossible to use direct i/o from Python without bindings.
"""

import os

cdef extern from 'stdlib.h':
    int posix_memalign(char **memptr, size_t alignment, size_t size) nogil
    void free(char *) nogil
    int errno

cdef extern from 'unistd.h':
    unsigned long pread(int, char *, unsigned long, unsigned long) nogil
    unsigned long pwrite(int, char *, unsigned long, unsigned long) nogil

cdef extern from 'string.h':
    void *memcpy(char *, char *, int) nogil

cdef class DirectFile:
    cdef int fd
    cdef int align

    def __init__(self, path, align=4096):
        self.fd = os.open(path, os.O_DIRECT | os.O_RDWR)
        if self.fd < 0:
            raise OSError(errno, 'Error opening')
        self.align = align

    def __dealloc__(self):
        os.close(self.fd)

    def pread(self, int offset, int length):
        cdef char *buf
        cdef int read_amt
        with nogil:
            posix_memalign(&buf, self.align, length)
            read_amt = pread(self.fd, buf, length, offset)
        if read_amt < 0:
            raise OSError(errno, 'Error reading')
        resp = buf[0:read_amt]
        free(buf)
        return resp

    def pwrite(self, offset, buf):
        cdef char *buf2
        cdef int write_amt
        cdef int length = len(buf)
        with nogil:
            posix_memalign(&buf2, self.align, length)
            memcpy(buf2, buf, length)
            write_amt = pwrite(self.fd, buf2, length, offset)
            free(buf2)
        if write_amt < 0:
            raise OSError(errno, 'Error writing')

    def __len__(self):
        return os.lseek(self.fd, 0, os.SEEK_END)
