#!/bin/sh

./pull_table_from_mr.py --tables "//home/ignat/mrserver" --destination="//statbox" --server "mrserver01e.stat.yandex.net" --job-count 20 --proxy mrproxy1e.mr.yandex.net --proxy mrproxy2e.mr.yandex.net --proxy mrproxy3e.mr.yandex.net

