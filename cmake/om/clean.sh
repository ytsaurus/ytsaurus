#!/bin/sh

name=`basename $0`
TEMP=`getopt -n $name -o D -l nodist -- "$@"`
if [ $? != 0 ]; then exit 1; fi
eval set -- "$TEMP"
while true; do
    case "$1" in
        -D|--nodist) nodist=yes; shift ;;
        --) shift; break ;;
        *) exit 0
    esac
done
 
rm -rf *~ *.pyc setuptools-*.egg *.egg-info om/*.pyc om_configuration/*.pyc
rm -rf build

if [ -z "$nodist" ]; then
    rm -rf dist
fi
