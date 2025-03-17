LIBRARY()

IF (YDB_ENABLE_PDISK_SHRED) 
    CFLAGS(
        -DENABLE_PDISK_SHRED
    )
ENDIF()
IF (YDB_DISABLE_PDISK_ENCRYPTION) 
    CFLAGS(
        -DDISABLE_PDISK_ENCRYPTION
    )
ENDIF()

PEERDIR(
    contrib/libs/lz4
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/protos
    contrib/ydb/library/actors/util
    contrib/ydb/library/actors/wilson
    library/cpp/containers/stack_vector
    library/cpp/deprecated/atomic
    library/cpp/lwtrace
    library/cpp/monlib/dynamic_counters/percentile
    library/cpp/monlib/service/pages
    util
    contrib/ydb/core/base
    contrib/ydb/core/base/services
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/blobstorage/crypto
    contrib/ydb/core/blobstorage/groupinfo
    contrib/ydb/core/blobstorage/lwtrace_probes
    contrib/ydb/core/control/lib
    contrib/ydb/core/driver_lib/version
    contrib/ydb/core/protos
    contrib/ydb/core/util
    contrib/ydb/library/pdisk_io
    contrib/ydb/library/schlab
    contrib/ydb/library/schlab/mon
    contrib/ydb/library/schlab/schine
)

GENERATE_ENUM_SERIALIZATION(blobstorage_pdisk_state.h)
GENERATE_ENUM_SERIALIZATION(blobstorage_pdisk_defs.h)


SRCS(
    blobstorage_pdisk.cpp
    blobstorage_pdisk_actor.cpp
    blobstorage_pdisk_blockdevice_async.cpp
    blobstorage_pdisk_completion_impl.cpp
    blobstorage_pdisk_delayed_cost_loop.cpp
    blobstorage_pdisk_driveestimator.cpp
    blobstorage_pdisk_drivemodel_db.cpp
    blobstorage_pdisk_impl.cpp
    blobstorage_pdisk_impl_http.cpp
    blobstorage_pdisk_impl_log.cpp
    blobstorage_pdisk_impl_metadata.cpp
    blobstorage_pdisk_internal_interface.cpp
    blobstorage_pdisk_log_cache.cpp
    blobstorage_pdisk_logreader.cpp
    blobstorage_pdisk_mon.cpp
    blobstorage_pdisk_params.cpp
    blobstorage_pdisk_requestimpl.cpp
    blobstorage_pdisk_syslogreader.cpp
    blobstorage_pdisk_sectorrestorator.cpp
    blobstorage_pdisk_tools.cpp
    blobstorage_pdisk_util_atomicblockcounter.cpp
    blobstorage_pdisk_util_flightcontrol.cpp
    blobstorage_pdisk_util_signal_event.cpp
    blobstorage_pdisk_writer.cpp
    drivedata_serializer.cpp
)

END()

RECURSE(
    mock
)

RECURSE_FOR_TESTS(
    ut
)
