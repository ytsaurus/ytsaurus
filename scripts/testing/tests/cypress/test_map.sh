#!/bin/bash
#% NUM_MASTERS = 3
#% NUM_HOLDERS = 0
#% SETUP_TIMEOUT = 3

yt set //map '{hello=world; list=[0;a;{}]; n=1}'
yt get //map

yt set //map/hello not_world
yt get //map

yt set //map/list/2/some value
yt get //map

yt remove //map/n
yt get //map

yt set //map/list []
yt get //map

yt set //map/list/+ {}
yt set //map/list/0/a 1
yt get //map

yt set //map/list/^0 {}
yt set //map/list/0/b 2
yt get //map

yt remove //map/hello
yt get //map

yt remove //map/list
yt get //map
