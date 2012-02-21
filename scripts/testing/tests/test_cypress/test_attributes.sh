#!/bin/bash
#% NUM_MASTERS = 3
#% NUM_HOLDERS = 0
#% SETUP_TIMEOUT = 3

echo '{do = set; path = "/root"; value = {nodes=[1; 2]} <attr=100;mode=rw>}' | ytdriver
echo '{do = get; path = "/root@}' | ytdriver
echo '{do = get; path = "/root@attr}' | ytdriver

echo '{do = remove; path = "/root@"}' | ytdriver
echo '{do = get; path = "/root@}' | ytdriver

echo '{do = remove; path = "/root/nodes"}' | ytdriver
echo '{do = get; path = "/root"' | ytdriver

echo '{do = set; path = "/root/2"; value = < author=ignat >}' | ytdriver
echo '{do = get; path = "/root/2"' | ytdriver
echo '{do = get; path = "/root/2@"' | ytdriver
echo '{do = get; path = "/root/2@author"' | ytdriver

#nested attributes
echo '{do = set; path = "/root/3"; value = <dir=<file=-100<>>>}' | ytdriver
echo '{do = get; path = "/root/3@"' | ytdriver
echo '{do = get; path = "/root/3@dir@"' | ytdriver
echo '{do = get; path = "/root/3@dir@file"' | ytdriver
echo '{do = get; path = "/root/3@dir@file@"' | ytdriver
