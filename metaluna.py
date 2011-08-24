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
REQUEST_LEN = struct.calcsize('!II8sQI')

NBD_SET_BLKSIZE = 43777
NBD_SET_SIZE_BLOCKS = 43783
NBD_SET_SOCK = 43776
NBD_DO_IT = 43779
NBD_CLEAR_QUE = 43781
NBD_CLEAR_SOCK = 43780


class BlockDevice(object):
    def __init__(self, size, device='/dev/nbd0'):
        self.cli, self.srv = socket.socketpair()
        self.nbd = os.open(device, os.O_RDWR)
        ioctl(self.nbd, NBD_SET_BLKSIZE, BLOCK_SIZE)
        ioctl(self.nbd, NBD_SET_SIZE_BLOCKS, size / BLOCK_SIZE)
        ioctl(self.nbd, NBD_SET_SOCK, self.srv.fileno())
        thread = threading.Thread(target=lambda: ioctl(self.nbd, NBD_DO_IT))
        thread.daemon = True
        thread.start()

    def serve(self):
        try:
            while True:
                try:
                    magic, op, handle, offset, length = struct.unpack('!II8sQI',
                                            self.cli.recv(REQUEST_LEN))
                    errno = 0
                    data = ''
                    if magic != NBD_REQUEST_MAGIC:
                        raise Exception('INVALID MAGIC')
                    elif op == NBD_CMD_READ:
                        data = self.read(offset, length)
                    elif op == NBD_CMD_WRITE:
                        self.write(offset, self.cli.recv(length))
                    elif op == NBD_CMD_DISC:
                        raise Exception('DISCONNECT REQUEST')
                    else:
                        raise Exception('UNKNOWN TYPE %s' % op)
                    self.cli.sendall(struct.pack('!II8s',
                                NBD_REPLY_MAGIC, errno, handle) + data)
                except (IOError, OSError), e:
                    self.cli.sendall(struct.pack('!II8s', NBD_REPLY_MAGIC,
                            e.errno, handle))
        finally:
            ioctl(self.nbd, NBD_CLEAR_QUE)
            ioctl(self.nbd, NBD_CLEAR_SOCK)
            self.cli.close()
            self.srv.close()

    def read(self, offset, length):
        raise NotImplementedError()

    def write(self, offset, buf):
        raise NotImplementedError()

