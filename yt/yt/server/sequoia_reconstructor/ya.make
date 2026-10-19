LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    helpers.cpp
    mapper.cpp
    program.cpp
    record_consumer.cpp
    reducer.cpp
    yt_connector.cpp
)

PEERDIR(
    library/cpp/getopt/small
    library/cpp/yson/node
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/library/operation_tracker
    yt/cpp/mapreduce/util
    yt/yt/core
    yt/yt/library/program
    yt/yt/library/fusion
    yt/yt/server/lib/io
    yt/yt/server/master
    yt/yt/ytlib
)

END()
