LIBRARY()

ADDINCL(
    contrib/ydb/public/sdk/cpp
)

SRCS(
    mon.cpp
    proxy_impl.cpp
    schemereq.cpp
    datareq.cpp
    describe.cpp
    proxy.cpp
    read_table_impl.cpp
    resolvereq.cpp
    rpc_long_tx.cpp
    snapshotreq.cpp
    commitreq.cpp
    upload_columns.cpp
    upload_rows_counters.cpp
    upload_rows_common_impl.cpp
    upload_rows.cpp
    global.cpp
)

GENERATE_ENUM_SERIALIZATION(read_table_impl.h)
GENERATE_ENUM_SERIALIZATION(upload_rows_counters.h)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/helpers
    contrib/ydb/library/actors/interconnect
    util/draft
    contrib/ydb/core/actorlib_impl
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/docapi
    contrib/ydb/core/engine
    contrib/ydb/core/formats
    contrib/ydb/core/grpc_services/local_rpc
    contrib/ydb/core/io_formats/arrow/scheme
    contrib/ydb/core/protos
    contrib/ydb/core/scheme
    contrib/ydb/core/sys_view/common
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx
    contrib/ydb/core/tx/balance_coverage
    contrib/ydb/core/tx/datashard
    contrib/ydb/core/tx/scheme_cache
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/core/tx/tx_allocator
    contrib/ydb/core/tx/tx_allocator_client
    contrib/ydb/library/aclib
    contrib/ydb/library/login
    contrib/ydb/library/mkql_proto/protos
    contrib/ydb/public/lib/base
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut_base_tenant
    ut_encrypted_storage
    ut_ext_tenant
    ut_schemereq
    ut_storage_tenant
)
