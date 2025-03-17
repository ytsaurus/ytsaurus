LIBRARY()

SRCS(
    debug_info.cpp
    defs.h
    kesus_quoter_proxy.cpp
    probes.cpp
    quoter_service.cpp
    quoter_service.h
    quoter_service_impl.h
)

PEERDIR(
    contrib/ydb/library/actors/core
    library/cpp/containers/ring_buffer
    contrib/ydb/core/base
    contrib/ydb/core/kesus/tablet
    contrib/ydb/core/tx/scheme_cache
    contrib/ydb/core/util
    yql/essentials/public/issue
    contrib/ydb/library/time_series_vec
)

END()

RECURSE(
    quoter_service_bandwidth_test
)

RECURSE_FOR_TESTS(
    ut
)
