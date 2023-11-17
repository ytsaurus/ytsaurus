LIBRARY()

SRCS(
    mon.cpp
    proxy_impl.cpp
    schemereq.cpp
    datareq.cpp
    describe.cpp
    proxy.cpp
    read_table_impl.cpp
    resolvereq.cpp
    snapshotreq.cpp
    commitreq.cpp
    upload_rows_common_impl.cpp
    upload_rows.cpp
)

GENERATE_ENUM_SERIALIZATION(read_table_impl.h)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/helpers
    library/cpp/actors/interconnect
    util/draft
    contrib/ydb/core/actorlib_impl
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/docapi
    contrib/ydb/core/engine
    contrib/ydb/core/formats
    contrib/ydb/core/grpc_services
    contrib/ydb/core/io_formats
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
    contrib/ydb/library/mkql_proto/protos
    contrib/ydb/public/lib/base
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut_base_tenant
    ut_encrypted_storage
    ut_ext_tenant
    ut_storage_tenant
)
