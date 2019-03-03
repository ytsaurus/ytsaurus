#!/bin/bash

name=$(date "+geodata-%Y%m%d-%H%M")
mkdir $name
pushd $name

echo "Downloading hierarchies"
curl -s 'http://geoexport.yandex.ru/?fields=id,parent_id,type,population&types=_all_' | tail -n+2 >regions_hierarchy.txt
curl -s 'http://geoexport.yandex.ru/?fields=id,parent_id,type,population&types=_all_&new_parents=977:187' | tail -n+2 >regions_hierarchy_ua.txt

download_regions_names() {
    echo "Downloading regions names for language $1 (ClickHouse name: $2)"
    curl -s "http://geoexport.yandex.ru/?fields=id,$1_name&types=_all_" | tail -n+2 >regions_names_$2.txt
}

echo "Dumping names..."
download_regions_names "ru" "ru"
download_regions_names "en" "en"
download_regions_names "uk" "ua"
download_regions_names "by" "by"
download_regions_names "kz" "kz"
download_regions_names "tr" "tr"

tar czf $name.tgz *

yt write-file //sys/clickhouse/geodata/$name <$name.tgz
yt link --force //sys/clickhouse/geodata/$name //sys/clickhouse/geodata/geodata.tgz

popd
