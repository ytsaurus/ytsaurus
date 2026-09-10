LIBRARY()

SRCS(
    yql_dq_clique.cpp
    yql_dq_clique_warmup_config.cpp
)

PEERDIR(
    yql/essentials/core/file_storage
    yql/essentials/providers/common/proto
    yt/yql/providers/dq/actors/yt
    yt/yql/providers/dq/config
)

END()
