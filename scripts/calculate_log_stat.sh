#!/bin/sh 

awk -F"[ \t\[]" '{a[$4 $5] += 1; s += 1 } END { for (i in a) { print i, a[i]/s } }' | sort -gk2
