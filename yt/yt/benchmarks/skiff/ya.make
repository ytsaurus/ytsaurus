PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/yt/library/process
    yt/yt/library/skiff_ext
    yt/yt/client
    yt/yt/library/formats
    library/cpp/getopt
)

SRCS(
    main.cpp
    experimental_yson_pull_format.cpp
)

END()
