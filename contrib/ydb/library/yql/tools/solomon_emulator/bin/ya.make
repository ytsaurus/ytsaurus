PY3_PROGRAM(solomon_emulator)

PEERDIR(
    contrib/ydb/library/yql/tools/solomon_emulator/lib
)

PY_SRCS(
    MAIN main.py
)

END()
