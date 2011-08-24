#!/usr/bin/python
import struct
import socket
import sys
import os
from Queue import Queue


PORT = 5000
CONN_MAGIC = 0x420281861253
REQUEST_MAGIC = 0x25609513
REPLY_MAGIC = 0x67446698
READ, WRITE, CLOSE = 0, 1, 2
HEADER_LEN = struct.calcsize('!II8sQI')


cdef extern from 'unistd.h':
    int pread(int fd, char *buf, int count, long offset)
    int pwrite(int fd, char *buf, int count, long offset)
    int errno

cdef extern from 'stdlib.h':
     void free(void *ptr)
     void *malloc(size_t size)

def pread_(int fd, int count, int offset):
    cdef char *buf = <char *>malloc(count)
    try:
        length = pread(fd, buf, count, offset)
        if length <= 0:
            raise IOError(errno, 'error on pread')
        return buf[:length]
    finally:
        free(buf)

def pwrite_(int fd, buf, offset):
    if pwrite(fd, buf, len(buf), offset) <= 0:
        raise IOError(errno, 'error on pread')


class Volume(object):
    def __init__(self, filename):
        self.fp = open(filename, 'rb+')
        self.fd = self.fp.fileno()

    def send(self, handle, payload='', errno=0):
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 1)
        self.sock.sendall(struct.pack('!II8s',
                (REPLY_MAGIC, errno, handle)) + payload)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 0)

    def recv(self, length):
        rv = []
        received = 0
        while received < length:
            rv.append(self.sock.recv(length - received))
            received += len(rv[-1])
        return ''.join(rv)

    def serve(self, sock):
        self.sock = sock
        volsize = os.lseek(self.fd, os.SEEK_END)
        sock.send(struct.pack('!8sQQ128s', 'NBDMAGIC', CONN_MAGIC, volsize, ''))
        while True:
            magic, op, handle, offset, length = struct.unpack('!II8sQI',
                                                    self.recv(HEADER_LEN))
            assert magic == REQUEST_MAGIC
            try:
                if op == READ:
                    self.send(handle, payload=pread_(self.fd, length, offset))
                elif op == WRITE:
                    pwrite_(self.fd, self.recv(length), offset)
                    self.send(handle)
                elif op == CLOSE:
                    sock.close()
                    return
                else:
                    print "ignored op", op, offset, length
            except (IOError, OSError), e:
                self.send(handle, errno=e.errno)


if __name__ == '__main__':
    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    lsock.bind(('', PORT))
    lsock.listen(5)
    while True:
        (sock, addr) = lsock.accept()
        Volume(sys.argv[1]).serve(sock)

