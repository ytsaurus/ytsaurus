# Use UNION instead PACKAGE to avoid artifact duplication; details in DEVTOOLSSUPPORT-15693
UNION()

INCLUDE(${ARCADIA_ROOT}/yt/packages/ya.make.common)

FROM_SANDBOX(
    # YT_ALL
    FILE 7119133217
    OUT ytserver-all RENAME result/ytserver-all
    EXECUTABLE
)

FROM_SANDBOX(
    # YT_LOCAL_BIN
    FILE 7119134042
    OUT yt_local RENAME result/yt_local
    EXECUTABLE
)

END()
