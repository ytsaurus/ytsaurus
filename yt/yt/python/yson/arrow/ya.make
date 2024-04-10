INCLUDE(../../pycxx.inc)

PY23_NATIVE_LIBRARY()

CXXFLAGS(
    ${PYCXX_FLAGS}
)

SRCS(
    GLOBAL parquet.cpp
    GLOBAL arrow_raw_iterator.cpp
)

PEERDIR(
    yt/yt/python/common
    yt/cpp/mapreduce/library/table_schema
    library/cpp/yson/node
    contrib/libs/pycxx
    contrib/libs/apache/arrow
)

ADDINCL(
    GLOBAL contrib/libs/pycxx
)

END()
