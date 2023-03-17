#!/bin/bash -ex

ya make -r
./generate_client_impl ../../client_impl_yandex.py

ya make -r -DOPENSOURCE
./generate_client_impl ../../client_impl.py
