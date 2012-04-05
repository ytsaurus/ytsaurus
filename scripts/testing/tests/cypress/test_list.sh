#!/bin/bash
#% NUM_MASTERS = 3
#% NUM_HOLDERS = 0

yt set //list '[1;2;"some string"]'
yt get //list

yt set //list/+ 100
yt get //list

yt set //list/^0 200
yt get //list

yt set //list/^0 500
yt get //list

yt set //list/2^ 1000
yt get //list

yt set //list/3 777
yt get //list

yt remove //list/4
yt get //list

yt remove //list/4
yt get //list

yt remove //list/0
yt get //list

yt set //list/+ last
yt get //list

yt set //list/^0 first
yt get //list
