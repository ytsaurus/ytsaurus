#!/usr/bin/env python

import yt.wrapper as yt

tables = []
for line in open("tables"):
    tables.append(line.strip())

list_name = "//home/ignat/tables_to_import"

yt.set(list_name, tables + yt.get(list_name))

