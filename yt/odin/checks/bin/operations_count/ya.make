PY3_PROGRAM(operations_count)

PEERDIR(
    yt/odin/checks/lib/check_runner
)
    
IF (NOT OPENSOURCE)
    PEERDIR(yt/odin/checks/lib/yandex_helpers)
ENDIF()

PY_SRCS(
    __main__.py
)

END()
