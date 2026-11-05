PY3TEST()

SUBSCRIBER(
    g:yt
    ilyaibraev
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/integration/YaMakeBoilerplateForTests.txt)

SRCDIR(yt/chyt/tests/server)

PY_SRCS(
    base.py
)

NO_CHECK_IMPORTS(base)

TEST_SRCS(
    test_access_log_viewer.py
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(
        cpu:36
        ram:48
    )
ELSE()
    REQUIREMENTS(
        ram:24
    )
ENDIF()

PEERDIR(
    library/python/port_manager
    yt/python/yt/clickhouse
    yt/python/yt/wrapper/testlib
)

DEPENDS(
    yt/chyt/server/bin
    yt/chyt/trampoline
    yt/microservices/access_log_viewer/http_service
    yt/yt/server/log_tailer/bin
)

IF (SANITIZER_TYPE == "thread")
    DEPENDS(
        contrib/libs/llvm20/tools/llvm-symbolizer
    )
ENDIF()

END()
