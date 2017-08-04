#!/bin/bash

echo "Chunks per path:"
./find_tables_to_merge.py --ignore-suppress-nightly-merge --print-only | ./aggregate_chunk_table_infomation.py
