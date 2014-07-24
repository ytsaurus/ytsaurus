#!/bin/sh -eu

index=0
for line in $(cat | grep -v "#;$"); do
    index=$((index + 1))
    echo "$line" >>file
    if [ "$(($index % 1000))" = "0" ]; then
        cat file | yt insert "$1" --format yson &
    fi
done

