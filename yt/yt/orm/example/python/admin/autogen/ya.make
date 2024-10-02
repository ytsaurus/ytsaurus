PY3_LIBRARY()

RUN_PROGRAM(
    yt/yt/orm/example/codegen render-yt-schema
    STDOUT_NOAUTO schema.py
)

PY_SRCS(
    schema.py
)

END()
