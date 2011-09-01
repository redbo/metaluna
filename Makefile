all:
	cython metaluna.pyx
	gcc metaluna.c -O3 -fPIC -shared -o metaluna.so -lpython2.6 -I/usr/include/python2.6
	cython directio.pyx
	gcc directio.c -O3 -fPIC -shared -o directio.so -lpython2.6 -I/usr/include/python2.6
