"""
This is a python class that implements virtual block devices by driving the
NBD kernel module directly.  It's meant for you to subclass and override
read and write, then call instantiate it and call .serve().
"""

import os
import socket
import threading
from fcntl import ioctl
import struct


BLOCK_SIZE = 4096

NBD_REQUEST_LEN = struct.calcsize('!II8sQI')
NBD_REQUEST_MAGIC = 0x25609513
NBD_REPLY_MAGIC = 0x67446698
NBD_CMD_READ = 0
NBD_CMD_WRITE = 1
NBD_CMD_DISC = 2

NBD_SET_SOCK = 43776
NBD_SET_BLKSIZE = 43777
NBD_SET_SIZE = 43778
NBD_DO_IT = 43779
NBD_CLEAR_SOCK = 43780
NBD_CLEAR_QUE = 43781
NBD_PRINT_DEBUG = 43782
NBD_SET_SIZE_BLOCKS = 43783
NBD_DISCONNECT = 43784


class BlockDevice(object):
    def __init__(self, size, device='/dev/nbd0'):
        self.size = size
        self.device = device

    def serve(self):
        cli, srv = socket.socketpair()
        nbd = os.open(self.device, os.O_RDWR)
        ioctl(nbd, NBD_SET_BLKSIZE, BLOCK_SIZE)
        ioctl(nbd, NBD_SET_SIZE_BLOCKS, self.size / BLOCK_SIZE)
        ioctl(nbd, NBD_SET_SOCK, srv.fileno())
        thread = threading.Thread(target=lambda: ioctl(nbd, NBD_DO_IT))
        thread.daemon = True
        thread.start()
        try:
            while True:
                try:
                    magic, op, handle, offset, length = struct.unpack('!II8sQI',
                                            cli.recv(NBD_REQUEST_LEN))
                    errno = 0
                    data = ''
                    if magic != NBD_REQUEST_MAGIC:
                        raise Exception('INVALID MAGIC')
                    elif op == NBD_CMD_READ:
                        data = self.read(offset, length)
                    elif op == NBD_CMD_WRITE:
                        self.write(offset, cli.recv(length))
                    elif op == NBD_CMD_DISC:
                        raise Exception('DISCONNECT REQUEST')
                    else:
                        raise Exception('UNKNOWN TYPE %s' % op)
                    cli.sendall(struct.pack('!II8s',
                                NBD_REPLY_MAGIC, errno, handle) + data)
                except (IOError, OSError), e:
                    cli.sendall(struct.pack('!II8s', NBD_REPLY_MAGIC,
                            e.errno, handle))
        finally:
            ioctl(nbd, NBD_DISCONNECT)
            ioctl(nbd, NBD_CLEAR_QUE)
            ioctl(nbd, NBD_CLEAR_SOCK)
            os.close(nbd)
            cli.close()
            srv.close()

    def read(self, offset, length):
        raise NotImplementedError()

    def write(self, offset, buf):
        raise NotImplementedError()

