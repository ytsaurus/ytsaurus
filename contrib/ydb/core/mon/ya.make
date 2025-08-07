LIBRARY()

SRCS(
    mon.cpp
    mon.h
    crossref.cpp
    crossref.h
)

PEERDIR(
    library/cpp/json
    library/cpp/lwtrace/mon
    library/cpp/protobuf/json
    library/cpp/string_utils/url
    contrib/ydb/core/base
    contrib/ydb/core/grpc_services/base
    contrib/ydb/core/mon/audit
    contrib/ydb/core/protos
    contrib/ydb/library/aclib
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/http
    yql/essentials/public/issue
    contrib/ydb/public/sdk/cpp/adapters/issue
    contrib/ydb/public/sdk/cpp/src/client/types/status
)

END()
