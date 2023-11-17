LIBRARY()

SRCS(
    txallocator__reserve.cpp
    txallocator__scheme.cpp
    txallocator_impl.cpp
    txallocator.cpp
)

PEERDIR(
    library/cpp/actors/helpers
    library/cpp/actors/interconnect
    contrib/ydb/core/actorlib_impl
    contrib/ydb/core/base
    contrib/ydb/core/engine/minikql
    contrib/ydb/core/protos
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
)

END()

RECURSE_FOR_TESTS(
    ut
)
