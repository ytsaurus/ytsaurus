PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    yt/cpp/mapreduce/client
    library/cpp/string_utils/levenshtein_diff
)

END()

