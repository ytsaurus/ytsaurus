#pragma once

#include "public.h"
#include "compact_table_schema.h"

#include <yt/yt/core/misc/async_expiring_cache.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

//! NB: This function is only for logging inside the TAsyncExpiringCache implementations.
void FormatValue(TStringBuilderBase* builder, const TCompactTableSchema& schema, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

class TTableSchemaCache
    : public TAsyncExpiringCache<TCompactTableSchema, NTableClient::TTableSchemaPtr>
{
public:
    explicit TTableSchemaCache(TAsyncExpiringCacheConfigPtr config);

    // Calls ConvertToHeavyTableSchema and caches the result.
    // Returns non-OK error in case of parsing errors. (Such errors are cached, too.)
    TErrorOr<NTableClient::TTableSchemaPtr> ConvertToHeavyTableSchemaAndCache(const TCompactTableSchema& compactTableSchema);

private:
    friend class TYsonTableSchemaCache;

    TFuture<NTableClient::TTableSchemaPtr> DoGet(
        const TCompactTableSchema& key,
        bool /*isPeriodicUpdate*/) noexcept override;

    // NB: This function is heavy, since it triggers deserialization from wire protobuf.
    // It is important to return copy of TIntrusivePtr for correct lifetime management of TTableSchema object.
    // Returns non-OK error in case of parsing errors.
    static TErrorOr<NTableClient::TTableSchemaPtr> ConvertToHeavyTableSchema(
        const TCompactTableSchema& compactTableSchema);
};

DEFINE_REFCOUNTED_TYPE(TTableSchemaCache)

////////////////////////////////////////////////////////////////////////////////

class TYsonTableSchemaCache
    : public TAsyncExpiringCache<TCompactTableSchema, NYson::TYsonString>
{
public:
    TYsonTableSchemaCache(const TWeakPtr<ITableManager>& weakTableManager, TYsonTableSchemaCacheConfigPtr config);

    void Reconfigure(const TYsonTableSchemaCacheConfigPtr& config);
private:
    const TWeakPtr<ITableManager> WeakTableManager_;
    bool EnableTableSchemaCache_;

    TFuture<NYson::TYsonString> DoGet(
        const TCompactTableSchema& key,
        bool /*isPeriodicUpdate*/) noexcept override;

    static TErrorOr<NYson::TYsonString> ConvertHeavySchemaToYsonString(const NTableClient::TTableSchemaPtr& heavySchema);
};

DEFINE_REFCOUNTED_TYPE(TYsonTableSchemaCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
