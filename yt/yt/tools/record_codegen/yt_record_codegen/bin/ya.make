PY3_PROGRAM(record-codegen)

PY_SRCS(
    NAMESPACE yt_record_codegen.bin
    __main__.py
)

PEERDIR(yt/yt/tools/record_codegen/yt_record_codegen)

END()
