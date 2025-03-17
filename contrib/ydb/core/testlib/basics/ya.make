LIBRARY()

SRCS(
    appdata.cpp
    helpers.cpp
    runtime.cpp
    services.cpp
)

PEERDIR(
    contrib/ydb/library/actors/dnsresolver
    library/cpp/regex/pcre
    library/cpp/testing/unittest
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage
    contrib/ydb/core/blobstorage/crypto
    contrib/ydb/core/blobstorage/nodewarden
    contrib/ydb/core/blobstorage/pdisk
    contrib/ydb/core/client/server
    contrib/ydb/core/control
    contrib/ydb/core/formats
    contrib/ydb/core/mind
    contrib/ydb/core/node_whiteboard
    contrib/ydb/core/quoter
    contrib/ydb/core/statistics/service
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/testlib/actors
    contrib/ydb/core/tx/columnshard
    contrib/ydb/core/tx/scheme_board
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/core/util
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/public/udf/service/exception_policy
    contrib/ydb/services/kesus
    contrib/ydb/services/persqueue_cluster_discovery
    contrib/ydb/services/ydb
)

YQL_LAST_ABI_VERSION()

IF (GCC)
    CFLAGS(
        -fno-devirtualize-speculatively
    )
ENDIF()

END()

RECURSE(
    default
    pg
)
