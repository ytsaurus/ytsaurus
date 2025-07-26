LIBRARY()

SRCS(
    defs.h
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/backpressure
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/blobstorage/crypto
    contrib/ydb/core/blobstorage/dsproxy
    contrib/ydb/core/blobstorage/groupinfo
    contrib/ydb/core/blobstorage/incrhuge
    contrib/ydb/core/blobstorage/lwtrace_probes
    contrib/ydb/core/blobstorage/nodewarden
    contrib/ydb/core/blobstorage/other
    contrib/ydb/core/blobstorage/pdisk
    contrib/ydb/core/blobstorage/storagepoolmon
    contrib/ydb/core/blobstorage/vdisk
)

IF (MSVC)
    CFLAGS(
        /wd4503
    )
ENDIF()

END()

RECURSE(
    backpressure
    base
    bridge
    crypto
    dsproxy
    groupinfo
    incrhuge
    lwtrace_probes
    nodewarden
    other
    pdisk
    storagepoolmon
    testing
    vdisk
)

RECURSE_FOR_TESTS(
    ut_blobstorage
    ut_group
    ut_mirror3of4
    ut_pdiskfit
    ut_testshard
    ut_vdisk
    ut_vdisk2
)
