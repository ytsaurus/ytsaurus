#!/bin/sh -eu

#export YT_CONFIG="./ytdriver.conf"

output_table="$1"

portion_number=0
index=0

insert() {
    portion_number=$(($portion_number + 1))
    #echo "Portion number $portion_number" >&2
    cat file | yt insert "$output_table" --format yson
    rm -rf file
    index=0
}

for line in $(cat | grep -v "#;$"); do
    index=$((index + 1))
    echo "$line" >>file
    if [ "$index" -gt "1000" ] && [[ "$line" == *";" ]]; then
        insert
    fi
done

insert

