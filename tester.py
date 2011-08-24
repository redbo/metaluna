from metaluna import BlockDevice

class MyBlockDevice(BlockDevice):
    def read(self, offset, length):
        print 'read', length
        return 'Z' * length

    def write(self, offset, buf):
        print 'write:', buf

x = MyBlockDevice(8192, '/dev/nbd0')
x.serve()

