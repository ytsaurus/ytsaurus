GTEST(unittester-library-farmhash)

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    farm_fingerprint_stability_ut.cpp
)

PEERDIR(
    library/cpp/yt/farmhash
)

END()
