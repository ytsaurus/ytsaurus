all:
	g++ -O2 -static-libgcc -L. -o tests/cpp_bin tests/bin.cpp

clean:
	rm -f *.pyc tests/*.pyc tests/cpp_bin

test: all
	PYTHONPATH=. python tests/test.py
	#./test_mapreduce.sh
