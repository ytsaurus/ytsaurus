LIBRARY()

SRCS(
    registry.cpp
    program.cpp
    builder.cpp
    resolver.cpp
)

PEERDIR(
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/protos
    contrib/ydb/library/formats/arrow/protos
    contrib/ydb/core/tablet_flat
    yql/essentials/minikql/comp_nodes
    yql/essentials/core/arrow_kernels/registry
    contrib/ydb/core/formats/arrow/program
)

YQL_LAST_ABI_VERSION()

END()
