PACKAGE()

INCLUDE(${ARCADIA_ROOT}/yt/packages/ya.make.common)

FROM_SANDBOX(
    # YT_ALL
    FILE 5493662223
    OUT ytserver-all RENAME result/ytserver-all
    EXECUTABLE
)

FROM_SANDBOX(
    # YT_LOCAL_BIN
    FILE 5493662780
    OUT yt_local RENAME result/yt_local
    EXECUTABLE
)

END()
