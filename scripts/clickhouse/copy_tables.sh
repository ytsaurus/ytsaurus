#!/bin/bash

source_cluster=$1
destination_cluster=$2

if [[ "${source_cluster}" == "" || "${destination_cluster}" == "" ]] ; then
    echo "Specify source cluster and destination cluster as command-line arguments"
    exit 1;
fi

function get_schema {
    yt get --proxy ${source_cluster} //home/arivkin/choyt/demo/$1/@schema --format "<format=text>yson"
}

function create_table {
    path=//home/arivkin/choyt/demo/$1
    compression_codec=$(yt get --proxy ${source_cluster} ${path}/@compression_codec);
    erasure_codec=$(yt get --proxy ${source_cluster} ${path}/@erasure_codec);
    schema=$(yt get --proxy ${source_cluster} ${path}/@schema --format "<format=text>yson");
    yt remove --proxy ${destination_cluster} ${path}
    yt create --proxy ${destination_cluster} table ${path} --attributes "{compresion_codec=${compression_codec}; erasure_codec=${erasure_codec}; schema=${schema}; optimize_for=scan; primary_medium=chyt_ssd_test; replication_factor=1;}"
}

for name in mobile_sorted_small mobile_sorted_medium mobile_sorted_small_unixtime mobile_sorted_medium_unixtime ;
do 
    echo "Copying ${name}"
    create_table $name
    yt remote-copy --proxy ${destination_cluster} --cluster ${source_cluster} --src //home/arivkin/choyt/demo/$name --dst //home/arivkin/choyt/demo/$name --spec '{job_io={table_writer={upload_replication_factor=1; min_upload_replication_factor=1}}}'
done
