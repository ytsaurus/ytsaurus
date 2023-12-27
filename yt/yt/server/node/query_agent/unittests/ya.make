GTEST(unittester-query-agent)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    replication_log_batch_reader_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/server/node
)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    TAG(
        ya:fat
    )
ELSE()
    SIZE(MEDIUM)
ENDIF()

TAG(
    ya:yt
)

END()
