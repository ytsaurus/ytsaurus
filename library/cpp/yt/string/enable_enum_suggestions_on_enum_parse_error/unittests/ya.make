GTEST(unittester-library-string-enum-suggestions)

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    enum_ut.cpp
)

PEERDIR(
    library/cpp/yt/string
    library/cpp/yt/string/enable_enum_suggestions_on_enum_parse_error
    library/cpp/testing/gtest
)

END()
