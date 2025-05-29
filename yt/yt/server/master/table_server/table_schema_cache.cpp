#include "table_schema_cache.h"

#include "config.h"
#include "private.h"
#include "table_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TableServerLogger;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TCompactTableSchema& schema, TStringBuf spec)
{
    FormatValue(builder, schema.AsWireProto(), spec);
}

////////////////////////////////////////////////////////////////////////////////

TTableSchemaCache::TTableSchemaCache(TAsyncExpiringCacheConfigPtr config)
    : TAsyncExpiringCache<TCompactTableSchema, TTableSchemaPtr>(
        std::move(config),
        TableServerLogger().WithTag("Cache: TableSchema"),
        TableServerProfiler().WithPrefix("/table_schema_cache"))
{ }

TTableSchemaPtr TTableSchemaCache::ConvertToHeavyTableSchemaAndCache(const TCompactTableSchema& compactTableSchema)
{
    auto schema = ConvertToHeavyTableSchema(compactTableSchema);
    Set(compactTableSchema, schema);
    return schema;
}

TFuture<TTableSchemaPtr> TTableSchemaCache::DoGet(
    const TCompactTableSchema& schema,
    bool /*isPeriodicUpdate*/) noexcept
{
    return BIND([schema] {
        return ConvertToHeavyTableSchema(schema);
    })
        .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker())
        .Run();
}

TTableSchemaPtr TTableSchemaCache::ConvertToHeavyTableSchema(const TCompactTableSchema& compactTableSchema)
{
    NTableClient::NProto::TTableSchemaExt protoSchema;
    YT_LOG_ALERT_AND_THROW_UNLESS(
        protoSchema.ParseFromString(compactTableSchema.AsWireProto()),
        "Failed to parse table schema from wire proto (WireProtobufSchema: %v)",
        compactTableSchema.AsWireProto());
    return NYT::FromProto<TTableSchemaPtr>(protoSchema);
}

////////////////////////////////////////////////////////////////////////////////

TYsonTableSchemaCache::TYsonTableSchemaCache(const TWeakPtr<ITableManager>& weakTableManager, TYsonTableSchemaCacheConfigPtr config)
    : TAsyncExpiringCache<TCompactTableSchema, TYsonString>(
        config,
        TableServerLogger().WithTag("Cache: YsonTableSchema"),
        TableServerProfiler().WithPrefix("/yson_table_schema_cache"))
    , WeakTableManager_(weakTableManager)
    , EnableTableSchemaCache_(config->CacheTableSchemaAfterConvertionToYson)
{ }

void TYsonTableSchemaCache::Reconfigure(const TYsonTableSchemaCacheConfigPtr& config)
{
    EnableTableSchemaCache_ = config->CacheTableSchemaAfterConvertionToYson;

    TAsyncExpiringCache::Reconfigure(config);
}

TFuture<TYsonString> TYsonTableSchemaCache::DoGet(
    const TCompactTableSchema& schema,
    bool /*isPeriodicUpdate*/) noexcept
{
    if (auto tableManager = WeakTableManager_.Lock(); EnableTableSchemaCache_ && tableManager) {
        return tableManager->GetHeavyTableSchemaAsync(schema)
            .Apply(BIND([schema] (const TErrorOr<TTableSchemaPtr>& heavySchemaOrError) {
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    heavySchemaOrError,
                    "Failed to get async table schema from %v",
                    schema);

                return ConvertToYsonString(heavySchemaOrError.Value());
            }));
    }

    return BIND([schema] {
        auto heavySchema = TTableSchemaCache::ConvertToHeavyTableSchema(schema);
        return ConvertToYsonString(heavySchema);
    })
        .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker())
        .Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
