#include "table_schema_cache.h"

#include <yt/yt/client/table_client/schema.h>

namespace NYT::NClickHouseServer {

using namespace NObjectClient;
using namespace NTableClient;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TCachedTableSchema::TCachedTableSchema(
    TObjectId schemaId,
    TTableSchemaPtr schema)
    : TSyncCacheValueBase(schemaId)
    , TableSchema_(std::move(schema))
    , Weight_(TableSchema_->GetMemoryUsage())
{ }

const TTableSchemaPtr& TCachedTableSchema::GetTableSchema() const
{
    return TableSchema_;
}

i64 TCachedTableSchema::GetWeight() const
{
    return Weight_;
}

////////////////////////////////////////////////////////////////////////////////

TTableSchemaCache::TTableSchemaCache(
    TSlruCacheConfigPtr config,
    TProfiler profiler)
    : TSyncSlruCacheBase(std::move(config), profiler.WithPrefix("/weight"))
    , HitCounter_(profiler.Counter("/hit"))
    , MissCounter_(profiler.Counter("/miss"))
{
    profiler.AddFuncGauge("/size", MakeStrong(this), [this] {
        return EntryCount_.load();
    });
}

TTableSchemaPtr TTableSchemaCache::Get(TObjectId schemaId)
{
    auto result = Find(schemaId);
    if (result) {
        HitCounter_.Increment();
        return result->GetTableSchema();
    }
    return nullptr;
}

void TTableSchemaCache::Insert(TObjectId schemaId, TTableSchemaPtr schema)
{
    auto cachedSchema = New<TCachedTableSchema>(schemaId, std::move(schema));
    if (TryInsert(std::move(cachedSchema))) {
        MissCounter_.Increment();
    }
}

void TTableSchemaCache::OnAdded(const TCachedTableSchemaPtr& /*value*/)
{
    EntryCount_.fetch_add(1);
}

void TTableSchemaCache::OnRemoved(const TCachedTableSchemaPtr& /*value*/)
{
    EntryCount_.fetch_sub(1);
}

i64 TTableSchemaCache::GetWeight(const TCachedTableSchemaPtr& value) const
{
    return value->GetWeight();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
