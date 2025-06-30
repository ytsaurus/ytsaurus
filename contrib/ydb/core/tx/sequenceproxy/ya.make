LIBRARY()

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/tx/scheme_cache
    contrib/ydb/core/tx/sequenceproxy/public
    contrib/ydb/core/tx/sequenceshard/public
)

SRCS(
    sequenceproxy.cpp
    sequenceproxy_allocate.cpp
    sequenceproxy_get_sequence.cpp
    sequenceproxy_impl.cpp
    sequenceproxy_resolve.cpp
)

END()

RECURSE(
    public
    ut
)

RECURSE_FOR_TESTS(
    ut
)
