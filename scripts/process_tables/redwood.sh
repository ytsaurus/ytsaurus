#!/bin/sh

./pull_table_from_mr.py --tables "//home/ignat/tables_to_import" --destination="//home/redwood" --server "redwood.yandex.ru" --fastbone --job-count 10

