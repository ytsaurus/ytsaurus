#!/bin/bash
core=`ls -1t /yt/disk1/core | head -n 1`
gdb ./ytserver /yt/disk1/core/$core