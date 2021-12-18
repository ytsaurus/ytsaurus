#pragma once

#include "public.h"

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/misc/sync_cache.h>

#include <util/datetime/base.h>

#include <atomic>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

using TSchemaCacheKey = std::pair<NObjectClient::TObjectId, NHydra::TRevision>;

////////////////////////////////////////////////////////////////////////////////

class TCachedTableSchemaWrapper
    : public TSyncCacheValueBase<TSchemaCacheKey, TCachedTableSchemaWrapper>
{
public:
    TCachedTableSchemaWrapper(
        TSchemaCacheKey schemaCacheKey,
        i64 schemaSize,
        TDuration requestTimeout);

    bool IsSet();

    bool TryRequestSchema();

    NTableClient::TTableSchemaPtr GetValue();

    void SetValue(NTableClient::TTableSchemaPtr cachedTableSchema);

    i64 GetWeight() const;

private:
    const TDuration RequestTimeout_;
    const i64 SchemaSize_;

    std::atomic<TInstant> NextRequestTime_;

    // NB: For concurrent access of CachedTableSchema_.
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    NTableClient::TTableSchemaPtr TableSchema_;

    bool CheckSchemaSet();
};

DEFINE_REFCOUNTED_TYPE(TCachedTableSchemaWrapper)

////////////////////////////////////////////////////////////////////////////////

//! Manages cached table schema and corresponding row comparer.
class TTableSchemaCache
    : public TSyncSlruCacheBase<TSchemaCacheKey, TCachedTableSchemaWrapper>
{
public:
    explicit TTableSchemaCache(TTableSchemaCacheConfigPtr config);

    TCachedTableSchemaWrapperPtr GetOrCreate(const TSchemaCacheKey& key, i64 schemaSize);

    void Configure(const TTableSchemaCacheDynamicConfigPtr& config);

private:
    const TTableSchemaCacheConfigPtr Config_;

    std::atomic<TDuration> TableSchemaCacheRequestTimeout_;

    i64 GetWeight(const TCachedTableSchemaWrapperPtr& value) const override;
};

DEFINE_REFCOUNTED_TYPE(TTableSchemaCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
