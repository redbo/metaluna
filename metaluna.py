#!/usr/bin/python
import os
import socket
import threading
from fcntl import ioctl
import struct


BLOCK_SIZE = 4096

NBD_REQUEST_MAGIC = 0x25609513
NBD_REPLY_MAGIC = 0x67446698
NBD_CMD_READ = 0
NBD_CMD_WRITE = 1
NBD_CMD_DISC = 2

NBD_SET_BLKSIZE = 43777
NBD_SET_SIZE_BLOCKS = 43783
NBD_SET_SOCK = 43776
NBD_DO_IT = 43779
NBD_CLEAR_QUE = 43781
NBD_CLEAR_SOCK = 43780


class Volume(object):
    def __init__(self, size, device='/dev/nbd0'):
        self.socketpair = socket.socketpair()
        self.nbd = os.open(device, os.O_RDWR)
        ioctl(self.nbd, NBD_SET_BLKSIZE, BLOCK_SIZE)
        block_count = size / BLOCK_SIZE
        ioctl(self.nbd, NBD_SET_SIZE_BLOCKS, block_count)
        ioctl(self.nbd, NBD_SET_SOCK, self.socketpair[0].fileno())
        print "starting"
        self.thread = threading.Thread(target=self._do_it)
        self.thread.daemon = True
        self.thread.start()
        print "started"

    def _do_it(self):
        ioctl(self.nbd, NBD_DO_IT)

    def serve(self):
        REQUEST_LEN = struct.calcsize('!II8sQI')
        try:
            while True:
                magic, op, handle, offset, length = struct.unpack('!II8sQI',
                                        self.socketpair[1].recv(REQUEST_LEN))
                errno = 0
                if magic != NBD_REQUEST_MAGIC:
                    raise Exception('INVALID MAGIC')
                elif op == NBD_CMD_READ:
                    data = self.read(offset, length)
                elif op == NBD_CMD_WRITE:
                    self.write(offset, self.socketpair[1].recv(length))
                    data = ''
                elif op == NBD_CMD_DISC:
                    raise Exception('DISCONNECT REQUEST')
                else:
                    raise Exception('UNKNOWN TYPE %s' % op)
                self.socketpair[1].sendall(struct.pack('!II8s',
                            NBD_REPLY_MAGIC, errno, handle) + data)
        finally:
            ioctl(self.nbd, NBD_CLEAR_QUE)
            ioctl(self.nbd, NBD_CLEAR_SOCK)
            self.socketpair[0].close()
            self.socketpair[1].close()

    def read(self, offset, length):
        return 'Z' * length

    def write(self, offset, buf):
        pass
