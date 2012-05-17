#!/bin/bash
mocha -R spec -s 250 -t 2500 \
    test_streams.js \
    test_http.js \
    test_utils.js
