#!/bin/bash -eux

mkdir yt_test_modules
g++ getnumber.cpp -shared -fPIC -o yt_test_modules/libgetnumber.so
g++ yt_test_lib.cpp -shared -L yt_test_modules/ -l getnumber -fPIC -o yt_test_modules/yt_test_dynamic_library.so
ya upload yt_test_modules
