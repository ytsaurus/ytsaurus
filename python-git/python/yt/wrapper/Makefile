all:
	g++ -O2 -static-libgcc -L. -o tests/cpp_bin tests/bin.cpp

clean:
	rm -rf *.pyc tests/*.pyc tests/cpp_bin tests/sandbox build dist YtWrapper.egg-info

test: all
	py.test -sv

test_mapreduce:
	./test_mapreduce.sh
