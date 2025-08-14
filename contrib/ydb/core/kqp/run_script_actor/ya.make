LIBRARY()

SRCS(
    kqp_run_script_actor.cpp
)

PEERDIR(
    library/cpp/protobuf/json
    contrib/ydb/core/base
    contrib/ydb/core/kqp/common/events
    contrib/ydb/core/kqp/executer_actor
    contrib/ydb/core/kqp/proxy_service/proto
    contrib/ydb/core/protos
    contrib/ydb/library/actors/core
    contrib/ydb/public/api/protos
)

YQL_LAST_ABI_VERSION()

END()
