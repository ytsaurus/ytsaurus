#!/bin/bash
mocha -R spec -s 1000 \
    test_basic.js \
    test_streams.js \
    test_http.js \
    test_utils.js
