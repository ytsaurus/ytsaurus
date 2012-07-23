all:
	g++ -O2 -static-libgcc -L. -o tests/cpp_bin tests/bin.cpp

clean:
	rm -rf *.pyc tests/*.pyc tests/cpp_bin tests/sandbox

test: all
	PYTHONPATH=.:${PYTHONPATH} python tests/test_all.py

test_mapreduce:
	./test_mapreduce.sh
