YQL_LIBRARY()

SRCS(
    interface.h
)

PEERDIR(
    yql/tools/yqlworker/executor/common
    yql/essentials/core/credentials
    yql/essentials/core/facade
    yql/essentials/core/file_storage
    yql/essentials/core/layers
    yql/essentials/core/qplayer/storage/interface
    yql/essentials/core/url_lister/interface
    yt/yql/providers/yt/lib/secret_masker
    yql/essentials/minikql
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/library/yql/providers/common/token_accessor/client
    yql/essentials/core
    yql/essentials/providers/common/proto
    yt/yql/providers/yt/provider
)

END()
