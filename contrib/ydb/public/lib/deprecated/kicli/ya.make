LIBRARY()

# See documentation
# https://wiki.yandex-team.ru/kikimr/techdoc/db/cxxapi/

SRCS(
    configurator.cpp
    dynamic_node.cpp
    error.cpp
    kicli.h
    kikimr.cpp
    query.cpp
    result.cpp
    schema.cpp
)

PEERDIR(
    contrib/libs/grpc
    contrib/ydb/library/actors/core
    library/cpp/threading/future
    contrib/ydb/core/protos
    contrib/ydb/library/aclib
    contrib/ydb/library/yql/public/ydb_issue
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/api/protos
    contrib/ydb/public/lib/base
    contrib/ydb/public/lib/deprecated/client
    contrib/ydb/public/lib/scheme_types
    contrib/ydb/public/lib/value
    yql/essentials/public/decimal
    yql/essentials/public/issue
)

END()
