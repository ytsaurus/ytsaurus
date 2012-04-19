#!/bin/bash
#% NUM_MASTERS = 3
#% NUM_HOLDERS = 0

yt set //root '<attr=100;mode=rw> {nodes=[1; 2]}'
yt get //root/@
yt get //root/@attr

yt remove //root/@
yt get //root/@

yt remove //root/nodes
yt get //root

echo 'changing attributes'
yt set //root/a '< author=ignat > []'
yt get //root/a
yt get //root/a/@
yt get //root/a/@author

yt set //root/a/@author not_ignat
yt get //root/a/@author 

echo 'nested attributes'
yt set //root/b '<dir = <file = <>-100> #> []' 
yt get //root/b/@
yt get //root/b/@dir/@
yt get //root/b/@dir/@file
yt get //root/b/@dir/@file/@
