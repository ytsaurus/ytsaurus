#pragma once

#include "private.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/misc/sync_cache.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TCachedTableSchema
    : public TSyncCacheValueBase<TGuid, TCachedTableSchema>
{
public:
    TCachedTableSchema(
        TGuid schemaId,
        NTableClient::TTableSchemaPtr schema);

    i64 GetWeight() const;
    const NTableClient::TTableSchemaPtr& GetTableSchema() const;

private:
    const NTableClient::TTableSchemaPtr TableSchema_;
    const i64 Weight_;
};

DEFINE_REFCOUNTED_TYPE(TCachedTableSchema)

////////////////////////////////////////////////////////////////////////////////

class TTableSchemaCache
    : public TSyncSlruCacheBase<TGuid, TCachedTableSchema>
{
public:
    explicit TTableSchemaCache(
        TSlruCacheConfigPtr config,
        NProfiling::TProfiler profiler = {});

    NTableClient::TTableSchemaPtr Get(TGuid schemaId);
    void Insert(TGuid schemaId, NTableClient::TTableSchemaPtr schema);

private:
    i64 GetWeight(const TCachedTableSchemaPtr& value) const override;
};

DEFINE_REFCOUNTED_TYPE(TTableSchemaCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
