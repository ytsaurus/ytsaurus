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

void FormatValue(TStringBuilderBase* builder, const TCompactTableSchemaPtr& schema, TStringBuf spec)
{
    if (schema) {
        FormatValue(builder, schema->AsWireProto(), spec);
    } else {
        builder->AppendString(TStringBuf("<null>"));
    }
}

////////////////////////////////////////////////////////////////////////////////

TTableSchemaCache::TTableSchemaCache(TAsyncExpiringCacheConfigPtr config)
    : TAsyncExpiringCache<TCompactTableSchemaPtr, TTableSchemaPtr>(
        std::move(config),
        TableServerLogger().WithTag("Cache: TableSchema"),
        TableServerProfiler().WithPrefix("/table_schema_cache"))
{ }

TErrorOr<TTableSchemaPtr> TTableSchemaCache::ConvertToHeavyTableSchemaAndCache(const TCompactTableSchemaPtr& compactTableSchema)
{
    auto schemaOrError = ConvertToHeavyTableSchema(compactTableSchema);
    Set(compactTableSchema, schemaOrError);
    return schemaOrError;
}

TFuture<TTableSchemaPtr> TTableSchemaCache::DoGet(
    const TCompactTableSchemaPtr& schema,
    bool /*isPeriodicUpdate*/) noexcept
{
    // NB: since DoGet and ConvertToHeavyTableSchemaAndCache are called in a
    // racy way (i.e. may be called interchangeably on different Hydra peers),
    // it's crucial for them to return identical results, including errors.

    return BIND([schema] {
        auto schemaOrError = ConvertToHeavyTableSchema(schema);
        return schemaOrError
            .ValueOrThrow();
    })
        .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker())
        .Run();
}

TErrorOr<TTableSchemaPtr> TTableSchemaCache::ConvertToHeavyTableSchema(const TCompactTableSchemaPtr& compactTableSchema)
{
    if (!compactTableSchema) {
        return TErrorOr<TTableSchemaPtr>(nullptr);
    }

    try {
        NTableClient::NProto::TTableSchemaExt protoSchema;
        if (!protoSchema.ParseFromString(compactTableSchema->AsWireProto())) {
            return TError(NCellServer::EErrorCode::CompactSchemaParseError,
                "Failed to parse table schema from wire proto");
        }

        // NB: FromProto may throw (invalid logical type is one reason).
        return NYT::FromProto<TTableSchemaPtr>(protoSchema);
    } catch (const std::exception& ex) {
        auto result = TError(NCellServer::EErrorCode::CompactSchemaParseError,
            "Failed to parse table schema");
        result <<= TError(ex);
        return result;
    }
}

////////////////////////////////////////////////////////////////////////////////

TYsonTableSchemaCache::TYsonTableSchemaCache(const TWeakPtr<ITableManager>& weakTableManager, TYsonTableSchemaCacheConfigPtr config)
    : TAsyncExpiringCache<TCompactTableSchemaPtr, TYsonString>(
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
    const TCompactTableSchemaPtr& schema,
    bool /*isPeriodicUpdate*/) noexcept
{
    if (auto tableManager = WeakTableManager_.Lock(); EnableTableSchemaCache_ && tableManager) {
        return tableManager->GetHeavyTableSchemaAsync(schema)
            .Apply(BIND([] (const TTableSchemaPtr& heavySchema) {
                return ConvertHeavySchemaToYsonString(heavySchema).ValueOrThrow();
            }));
    }

    return BIND([schema] {
        auto heavySchemaOrError = TTableSchemaCache::ConvertToHeavyTableSchema(schema);
        return ConvertHeavySchemaToYsonString(heavySchemaOrError.ValueOrThrow()).ValueOrThrow();
    })
        .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker())
        .Run();
}

/*static*/ TErrorOr<TYsonString> TYsonTableSchemaCache::ConvertHeavySchemaToYsonString(
    const NTableClient::TTableSchemaPtr& heavySchema)
{
    try {
        return ConvertToYsonString(heavySchema);
    } catch (const std::exception& ex) {
        auto result = TError(NCellServer::EErrorCode::CompactSchemaParseError,
            "Failed to convert table schema to yson string");
        result <<= TError(ex);
        return result;
    }

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
