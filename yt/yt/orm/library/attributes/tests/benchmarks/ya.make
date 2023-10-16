G_BENCHMARK()

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

SRCS(
    scalar_attribute.cpp
)

PEERDIR(
    yt/yt/orm/library/attributes/tests/proto

    yt/yt/orm/library/attributes

    library/cpp/testing/gtest_extensions
)

END()
