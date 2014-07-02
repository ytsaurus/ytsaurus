all:
	g++ -O2 -static-libgcc -L. -o tests/files/cpp_bin tests/files/bin.cpp

clean:
	rm -rf *.pyc tests/*.pyc tests/files/cpp_bin tests/sandbox/* build dist YtWrapper.egg-info

test: all
	py.test -sv

test_mapreduce:
	./test_mapreduce.sh

killytservers:
	ps aux | grep ytserver | awk '{print $$2}' | xargs kill