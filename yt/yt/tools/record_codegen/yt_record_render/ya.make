PY3_PROGRAM()

PEERDIR(
    yt/yt/tools/record_codegen/yt_record_render/lib

    contrib/python/click
)

PY_SRCS(
    NAMESPACE yt_record_render
    __main__.py
)

END()
