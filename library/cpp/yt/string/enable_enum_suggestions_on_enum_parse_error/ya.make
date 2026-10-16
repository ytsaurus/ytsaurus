LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    GLOBAL enum_suggestions.cpp
)

PEERDIR(
    library/cpp/string_utils/levenshtein_diff
    library/cpp/yt/string
)

END()

RECURSE_FOR_TESTS(
    unittests
)
