#!/bin/bash

source_cluster=$1
destination_cluster=$2

if [[ "${source_cluster}" == "" || "${destination_cluster}" == "" ]] ; then
    echo "Specify source cluster and destination cluster as command-line arguments"
    exit 1;
fi

function create_table {
    src=$1
    dst=$2
    primary_medium=$3
    echo "Creating table ${destination_cluster}:${dst} based on ${source_cluster}:${src}"
    compression_codec=$(yt get --proxy ${source_cluster} ${src}/@compression_codec);
    erasure_codec=$(yt get --proxy ${source_cluster} ${src}/@erasure_codec);
    schema=$(yt get --proxy ${source_cluster} ${src}/@schema --format "<format=text>yson");
    yt remove --proxy ${destination_cluster} ${dst}
    yt create --proxy ${destination_cluster} table ${dst} --attributes "{compresion_codec=${compression_codec}; erasure_codec=${erasure_codec}; schema=${schema}; optimize_for=scan; primary_medium=$primary_medium; replication_factor=1;}"
}

function copy_table {
    src=$1
    dst=$2
    echo "Copying ${source_cluster}:${src} -> ${destination_cluster}:${dst}"
    yt remote-copy --proxy ${destination_cluster} --cluster ${source_cluster} --src $1 --dst $2 --spec '{job_io={table_writer={upload_replication_factor=1; min_upload_replication_factor=1}}}'
}

function merge_table {
    src=$1
    dst=$2
    echo "Mergine ${source_cluster}:${src} -> ${destination_cluster}:${dst}"
    yt merge --mode ordered --spec '{force_transform=%true}' --src ${src} --dst ${dst}
}

for name in mobile_sorted_small mobile_sorted_medium mobile_sorted_small_unixtime mobile_sorted_medium_unixtime
do 
    create_table //home/arivkin/choyt/demo/$name //home/arivkin/choyt/demo/$name chyt_ssd_test
    copy_table //home/arivkin/choyt/demo/$name //home/arivkin/choyt/demo/$name chyt_ssd_test
done

for name in gta_mobile_hypercube_sorted_date_brotli_8 gta_mobile_hypercube_sorted_date_lz4;
do 
    create_table //home/arivkin/choyt/demo/$name //home/arivkin/choyt/demo/$name default
    copy_table //home/arivkin/choyt/demo/$name //home/arivkin/choyt/demo/$name default
done

create_table //home/arivkin/choyt/demo/mobile_sorted_small //home/arivkin/choyt/demo/mobile_sorted_small_uncompressed
yt set --proxy ${destination_cluster} //home/arivkin/choyt/demo/mobile_sorted_small_uncompressed/@compression_codec none
create_table //home/arivkin/choyt/demo/mobile_sorted_medium //home/arivkin/choyt/demo/mobile_sorted_medium_uncompressed
yt set --proxy ${destination_cluster} //home/arivkin/choyt/demo/mobile_sorted_medium_uncompressed/@compression_codec none
merge_table //home/arivkin/choyt/demo/mobile_sorted_small //home/arivkin/choyt/demo/mobile_sorted_small_uncompressed
merge_table //home/arivkin/choyt/demo/mobile_sorted_medium //home/arivkin/choyt/demo/mobile_sorted_medium_uncompressed

