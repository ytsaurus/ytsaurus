LIBRARY()

SRCS(
    registry.cpp
    program.cpp
)

PEERDIR(
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/protos
    contrib/ydb/core/tablet_flat
    contrib/ydb/library/yql/minikql/comp_nodes/llvm
    contrib/ydb/library/yql/core/arrow_kernels/registry
)

YQL_LAST_ABI_VERSION()

END()
