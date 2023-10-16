LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/library/table_schema
)

SRCS(
    yt_lambda.cpp
    field_copier.cpp
    wrappers.cpp
)

END()
