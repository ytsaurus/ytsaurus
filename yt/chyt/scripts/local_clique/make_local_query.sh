#!/bin/bash

query=$(cat query.sql)
curl "localhost:4245/?max_threads=1" -H "X-ClickHouse-User: $USER" -d "$(cat query.sql)"
