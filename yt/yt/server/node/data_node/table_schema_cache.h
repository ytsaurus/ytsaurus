#pragma once

#include "public.h"

#include <yt/server/node/tablet_node/sorted_dynamic_comparer.h>

#include <yt/client/object_client/public.h>

#include <yt/core/misc/sync_cache.h>

#include <util/datetime/base.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

using TSchemaCacheKey = std::pair<NObjectClient::TObjectId, NHydra::TRevision>;

struct TCachedTableSchema
    : public TIntrinsicRefCounted
{
    TCachedTableSchema(
        NTableClient::TTableSchemaPtr tableSchema,
        NTabletNode::TSortedDynamicRowKeyComparer rowKeyComparer);

    NTableClient::TTableSchemaPtr TableSchema;
    NTabletNode::TSortedDynamicRowKeyComparer RowKeyComparer;
};

DEFINE_REFCOUNTED_TYPE(TCachedTableSchema)

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

    TCachedTableSchemaPtr GetValue();

    void SetValue(TCachedTableSchemaPtr cachedTableSchema);

    i64 GetWeight() const;

private:
    const TDuration RequestTimeout_;
    const i64 SchemaSize_;

    std::atomic<TInstant> NextRequestTime_;

    // NB: For concurrent access of CachedTableSchema_.
    NConcurrency::TReaderWriterSpinLock SpinLock_;
    TCachedTableSchemaPtr CachedTableSchema_;

    bool CheckSchemaSet();
};

DEFINE_REFCOUNTED_TYPE(TCachedTableSchemaWrapper)

////////////////////////////////////////////////////////////////////////////////

//! Manages cached table schema and corresponding row comparer.
class TTableSchemaCache
    : public TSyncSlruCacheBase<TSchemaCacheKey, TCachedTableSchemaWrapper>
{
public:
    explicit TTableSchemaCache(const TTableSchemaCacheConfigPtr& config);

    TCachedTableSchemaWrapperPtr GetOrCreate(const TSchemaCacheKey& key, i64 schemaSize);

private:
    const TDuration TableSchemaCacheRequestTimeout_;

    virtual i64 GetWeight(const TCachedTableSchemaWrapperPtr& value) const override;
};

DEFINE_REFCOUNTED_TYPE(TTableSchemaCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
