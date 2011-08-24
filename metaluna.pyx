#!/usr/bin/python
import struct
import sys
import os
import socket
import threading


BLOCK_SIZE = 4096

cdef extern from 'nbd.h':
    int NBD_SET_SOCK, NBD_SET_BLKSIZE, NBD_SET_SIZE, NBD_DO_IT,\
        NBD_CLEAR_SOCK, NBD_CLEAR_QUE, NBD_PRINT_DEBUG,\
        NBD_SET_SIZE_BLOCKS, NBD_DISCONNECT, NBD_SET_TIMEOUT
    int NBD_CMD_READ, NBD_CMD_WRITE, NBD_CMD_DISC
    int NBD_REQUEST_MAGIC, NBD_REPLY_MAGIC
    struct nbd_request:
        unsigned int magic
        int op
        char handle[8]
        long offset
        int length
    struct nbd_reply:
        unsigned int magic
        int error
        char handle[8]

cdef extern from 'arpa/inet.h':
    unsigned int htonl(unsigned int hostlong)

cdef extern from 'sys/ioctl.h':
    int ioctl(int d, int request, ...) nogil

cdef extern from 'unistd.h':
    int read(int fd, void *buf, int count) nogil
    int write(int fd, void *buf, int count) nogil

cdef extern from 'string.h':
    void *memcpy(void *dest, void *src, int n)

class Volume(object):
    def __init__(self, size, device):
        self.socketpair = socket.socketpair()
        self.nbd = os.open(device, os.O_RDWR)
        ioctl(self.nbd, NBD_SET_BLKSIZE, <unsigned long>BLOCK_SIZE)
        block_count = size / BLOCK_SIZE
        ioctl(self.nbd, NBD_SET_SIZE_BLOCKS, <unsigned long>block_count)
        ioctl(self.nbd, NBD_SET_SOCK, <int>self.socketpair[0].fileno())
        self.thread = threading.Thread(target=self._do_it)
        self.thread.daemon = True
        self.thread.start()

    def _do_it(self):
        cdef int nbd = self.nbd
        with nogil:
            ioctl(nbd, NBD_DO_IT)

    def serve(self):
        cdef int fd = self.socketpair[1].fileno()
        cdef nbd_request req
        cdef nbd_reply rep
        rep.magic = htonl(NBD_REPLY_MAGIC)
        rep.error = 0
        try:
            while True:
                read_len = read(fd, <void *>&req, sizeof(nbd_request))
                if read_len <= 0:
                    raise Exception('INVALID READ')
                req.length = htonl(req.length)
                req.offset = htonl(req.offset)
                memcpy(<void *>&rep.handle, <void *>&req.handle, 8)
                if htonl(req.magic) != NBD_REQUEST_MAGIC:
                    raise Exception('INVALID MAGIC')
                elif req.op == NBD_CMD_READ:
                    data = self.read(req.offset, req.length)
                elif req.op == NBD_CMD_WRITE:
                    self.write(req.offset, os.read(fd, req.length))
                    data = ''
                elif req.op == NBD_CMD_DISC:
                    raise Exception('DISCONNECT REQUEST')
                else:
                    raise Exception('UNKNOWN TYPE %s' % req.op)
                write(fd, <void *>&rep, sizeof(nbd_reply))
                write(fd, <void *><char *>data, len(data))
        finally:
            ioctl(self.nbd, NBD_CLEAR_QUE)
            ioctl(self.nbd, NBD_CLEAR_SOCK)
            self.socketpair[0].close()
            self.socketpair[1].close()

    def read(self, offset, length):
        return 'Z' * length

    def write(self, offset, buf):
        pass

