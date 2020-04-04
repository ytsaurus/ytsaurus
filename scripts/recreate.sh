#!/bin/bash

table=$1
[[ -z "$table" ]] && exit 1

set -ex

attribute_keys=(schema key_columns $(yt get "$table/@user_attribute_keys" --format dsv))

yt unmount_table "$table"

for attribute_key in "${attribute_keys[@]}"
do
  yt get "$table/@$attribute_key" > saved.$attribute_key
done

yt remove "$table"
yt create table "$table"

for attribute_key in "${attribute_keys[@]}"
do
  yt set "$table/@$attribute_key" < saved.$attribute_key
done

yt mount_table "$table"

