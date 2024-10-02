LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

PEERDIR(
    yt/yt/orm/client/misc

    yt/yt/client

    yt/yt/core

    library/cpp/containers/bitset
    library/cpp/yson/node
    library/cpp/yt/small_containers
    library/cpp/yt/string
)

SRCS(
    acl.cpp
    helpers.cpp
    key.cpp
    object_filter.cpp
    public.cpp
    registry.cpp
    tags.cpp
    transaction_context.cpp
    type.cpp
)

END()

RECURSE_FOR_TESTS(unittests)
