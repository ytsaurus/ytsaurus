clean:
	rm -rf *.pyc tests/*.pyc tests.sandbox/*

test:
	py.test -sv

test_mapreduce:
	./test_mapreduce.sh

kill_yt:
	ps aux | grep ytserver | awk '{print $$2}' | xargs kill
	ps aux | grep node | awk '{print $$2}' | xargs kill
