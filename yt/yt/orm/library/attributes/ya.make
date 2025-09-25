LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

SRCS(
    attribute_path.cpp
    helpers.cpp
    merge_attributes.cpp
    patch_unwrapping_consumer.cpp
    path_visitor.cpp
    proto_visitor.cpp
    scalar_attribute.cpp
    unwrapping_consumer.cpp
    wire_string.cpp
    ytree.cpp
)

PEERDIR(
    yt/yt/orm/library/mpl

    yt/yt/core

    library/cpp/containers/bitset
)

END()

RECURSE_FOR_TESTS(
    tests
)
