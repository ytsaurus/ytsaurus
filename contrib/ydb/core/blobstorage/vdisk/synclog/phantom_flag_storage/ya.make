LIBRARY()

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/interconnect
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/blobstorage/vdisk/common
    contrib/ydb/core/util
)

SRCS(
    phantom_flag_storage_builder.cpp
    phantom_flag_storage_snapshot.cpp
    phantom_flag_storage_state.cpp
    phantom_flag_thresholds.cpp
)

END()

# TODO: tests
# RECURSE_FOR_TESTS(
#     ut
# )
