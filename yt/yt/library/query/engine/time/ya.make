LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    dates_lut_localtime.cpp
    dates_lut_utc.cpp
    dates.cpp
)

PEERDIR(
    yt/yt/library/query/engine_api
    library/cpp/timezone_conversion
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/library/query/engine/peerdir_policy.inc)

END()

RECURSE_FOR_TESTS(
    unittests
)
