LIBRARY()

SRCS(
    compacted_blob_constructor.cpp
    indexed_blob_constructor.cpp
    blob_constructor.cpp
    put_status.cpp
    write_controller.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/tablet_flat/protos
    contrib/ydb/core/blobstorage/vdisk/protos
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/formats/arrow


    library/cpp/actors/core
)

YQL_LAST_ABI_VERSION()

END()
