LIBRARY()

SRCS(
    single_thread_ic_mock.h
    single_thread_ic_mock.cpp
    testactorsys.h
    testactorsys.cpp
    defs.h
)

PEERDIR(
    contrib/ydb/apps/version
    # library/cpp/testing/unittest
    contrib/ydb/core/blobstorage/backpressure
    contrib/ydb/core/blobstorage/groupinfo
    contrib/ydb/core/blobstorage/pdisk/mock
    contrib/ydb/core/blobstorage/vdisk
    contrib/ydb/core/blobstorage/vdisk/common
    contrib/ydb/core/tx/scheme_board
    # contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/interconnect/mock
    contrib/ydb/library/actors/util
    contrib/ydb/library/actors/wilson
    library/cpp/containers/stack_vector
    library/cpp/html/escape
    library/cpp/ipmath
    library/cpp/json
    library/cpp/lwtrace
    library/cpp/monlib/dynamic_counters
    library/cpp/random_provider
    contrib/ydb/core/base
    contrib/ydb/core/protos
    library/cpp/deprecated/atomic
    contrib/ydb/library/yverify_stream
)

END()
