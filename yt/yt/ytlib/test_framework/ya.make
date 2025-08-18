LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    test_connection.cpp
)

PEERDIR(
    yt/yt/library/query/engine
    yt/yt/library/query/unittests/helpers
    yt/yt/library/query/row_comparer

    yt/yt/build

    yt/yt/core
    yt/yt/core/test_framework

    yt/yt/ytlib

    contrib/libs/sparsehash

    library/cpp/http/server
)


END()
