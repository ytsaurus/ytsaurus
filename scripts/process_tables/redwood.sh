#!/bin/sh

./import_table_from_mr.py --tables "//home/ignat/tables_to_import" --destination="//home/redwood" --server "redwood.yandex.ru" --codec "gzip_best_compression"

