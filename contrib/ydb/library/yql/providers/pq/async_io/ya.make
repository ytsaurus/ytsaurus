LIBRARY()

SRCS(
    dq_pq_meta_extractor.cpp
    dq_pq_rd_read_actor.cpp
    dq_pq_read_actor.cpp
    dq_pq_read_actor_base.cpp
    dq_pq_write_actor.cpp
    probes.cpp
)

PEERDIR(
    library/cpp/protobuf/interop
    contrib/ydb/core/fq/libs/graph_params/proto
    contrib/ydb/core/fq/libs/protos
    contrib/ydb/core/fq/libs/row_dispatcher
    contrib/ydb/library/actors/log_backend
    contrib/ydb/library/yql/dq/actors/compute
    yql/essentials/minikql/computation
    contrib/ydb/library/yql/providers/common/token_accessor/client
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/pq/common
    contrib/ydb/library/yql/providers/pq/proto
    yql/essentials/public/types
    yql/essentials/utils/log
    contrib/ydb/public/sdk/cpp/adapters/issue
    contrib/ydb/public/sdk/cpp/src/client/federated_topic
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/public/sdk/cpp/src/client/topic
    contrib/ydb/public/sdk/cpp/src/client/types/credentials
)

YQL_LAST_ABI_VERSION()

END()
