GTEST(unittester-yt-orm-library)

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

SRCS(
    attribute_path_ut.cpp
    attribute_processing_ut.cpp
    merge_attributes_ut.cpp
    patch_unwrapping_consumer_ut.cpp
    scalar_attribute_ut.cpp
    unwrapping_consumer_ut.cpp
    wire_string_ut.cpp
    yson_builder_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/orm/library/attributes/tests/proto

    yt/yt/orm/library/attributes

    yt/yt/core/test_framework
)

END()
