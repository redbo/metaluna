from metaluna import BlockDevice

class MyVolume(BlockDevice):
    def read(self, offset, length):
        return 'Z' * length

    def write(self, offset, buf):
        pass

x = MyVolume(8192, '/dev/nbd11')
x.serve()

