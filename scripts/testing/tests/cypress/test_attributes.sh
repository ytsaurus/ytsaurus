#!/bin/bash
#% NUM_MASTERS = 3
#% NUM_HOLDERS = 0

yt set //root '{nodes=[1; 2]} <attr=100;mode=rw>'
yt get //root@
yt get //root@attr

yt remove //root@
yt get //root@

yt remove //root/nodes
yt get //root

echo 'changing attributes'
yt set //root/2 '[] < author=ignat >'
yt get //root/2
yt get //root/2@
yt get //root/2@author

yt set //root/2@author not_ignat
yt get //root/2@author 

echo 'nested attributes'
yt set //root/3 '[] <dir=<file=-100<>>>' 
yt get //root/3@
yt get //root/3@dir@
yt get //root/3@dir@file
yt get //root/3@dir@file@
