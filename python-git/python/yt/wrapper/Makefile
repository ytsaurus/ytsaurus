all:
	g++ -O2 -static-libgcc -L. -o tests/files/cpp_bin tests/files/bin.cpp

clean:
	rm -rf *.pyc tests/*.pyc tests/files/cpp_bin tests/sandbox/* build dist YtWrapper.egg-info

test: all
	py.test -sv -k "not select"

test_mapreduce:
	./test_mapreduce.sh
