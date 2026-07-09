LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    GLOBAL register.cpp
    fetcher.cpp
    joiner.cpp
    range.cpp
    servicelog.cpp
    spec.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/connectors/common
    yt/yt/flow/library/cpp/resources
    yt/yt/core
    yt/yt/client
)

END()

IF (NOT SANITIZER_TYPE OR SANITIZER_TYPE != thread)
    RECURSE_FOR_TESTS(
        unittests
    )
ENDIF()
