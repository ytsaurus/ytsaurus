LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SUBSCRIBER(g:yt)

SRCS(
    chunk_reader.cpp
    chunk_writer.cpp
    config.cpp
)

PEERDIR(
    yt/yt/server/lib/io
    
    yt/yt/ytlib

    library/cpp/digest/md5
)

END()

RECURSE_FOR_TESTS(
    unittests
)
