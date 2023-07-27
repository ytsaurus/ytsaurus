#!/bin/sh

ya make -r
./gen_corpus data
ya upload --ttl=inf data
