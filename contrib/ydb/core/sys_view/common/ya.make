LIBRARY()

SRCS(
    common.h
    events.h
    keys.h
    path.h
    scan_actor_base_impl.h
    registry.cpp
    resolver.cpp
    utils.h
    processor_scan.h
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/core/tablet_flat
    library/cpp/deprecated/atomic
    yql/essentials/parser/pg_wrapper/interface
)

END()
