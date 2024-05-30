LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

SRCS(
    attribute_path.cpp
    helpers.cpp
    merge_attributes.cpp
    proto_visitor.cpp
    scalar_attribute.cpp
    unwrapping_consumer.cpp
    ytree.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/client
)

END()

RECURSE_FOR_TESTS(
    tests
)
