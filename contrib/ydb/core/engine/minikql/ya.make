LIBRARY()

SRCS(
    flat_local_minikql_host.h
    flat_local_tx_factory.cpp
    minikql_engine_host.cpp
    minikql_engine_host.h
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/client/minikql_compile
    contrib/ydb/core/engine
    contrib/ydb/core/formats
    contrib/ydb/core/tablet_flat
    yql/essentials/parser/pg_wrapper/interface
)

YQL_LAST_ABI_VERSION()

END()
