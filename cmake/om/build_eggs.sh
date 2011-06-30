#!/bin/sh -e

versions='2.4 2.5 2.6'
copy_to=maitai:/usr/local/www/om/

name=`basename $0`
TEMP=`getopt -n $name -o 12c -l om,configuration,copy -- "$@"`
if [ $? != 0 ]; then exit 1; fi
eval set -- "$TEMP"
while true; do
    case "$1" in
        -1|--om) om=yes; shift ;;
        -2|--configuration) conf=yes; shift ;;
        -c|--copy) copy=yes; shift ;;
        --) shift; break ;;
    esac
done

if [ -z "$om" -a -z "$conf" ]; then
    om=yes
    conf=yes
fi
 
./clean.sh

for x in $versions; do
    python="/usr/bin/env python$x"
    if $python -V >/dev/null 2>&1; then
        if [ -n "$om" ]; then
            echo -n "Building om egg for Python $x... "
            $python setup.py clean --all >/dev/null
            $python setup.py bdist_egg >/dev/null
            echo done.
        fi
        if [ -n "$conf" ]; then
            echo -n "Building om_configuration egg for Python $x... "
            $python setup_configuration.py clean --all >/dev/null
            $python setup_configuration.py bdist_egg >/dev/null
            echo done.
        fi
    fi
done

if [ -n "$copy" ]; then
    echo -n "Copying dist/* to $copy_to... "
    scp dist/* $copy_to >/dev/null
    echo done.
    distclean=yes
fi

if [ -n "$distclean" ]; then
    echo -n "Cleaning... "
    ./clean.sh
else
    echo -n "Cleaning, but leaving dist/*... "
    ./clean.sh --nodist
fi
echo done.
