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
    contrib/libs/pycxx
    contrib/libs/apache/arrow
)

ADDINCL(
    GLOBAL contrib/libs/pycxx
)

END()
