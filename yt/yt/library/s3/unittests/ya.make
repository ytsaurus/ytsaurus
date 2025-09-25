GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    client_ut.cpp
    crypto_helpers_ut.cpp
    http_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/core
    yt/yt/library/s3
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/local_s3_recipe/recipe.inc)

END()
