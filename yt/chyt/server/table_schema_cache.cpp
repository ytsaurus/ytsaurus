#include "table_schema_cache.h"

#include <yt/yt/client/table_client/schema.h>

namespace NYT::NClickHouseServer {

using namespace NObjectClient;
using namespace NTableClient;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TCachedTableSchema::TCachedTableSchema(
    TGuid schemaId,
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
    : TSyncSlruCacheBase(std::move(config), std::move(profiler))
{ }

TTableSchemaPtr TTableSchemaCache::Get(TGuid schemaId)
{
    auto res = Find(schemaId);
    if (res) {
        return res->GetTableSchema();
    }
    return nullptr;
}

void TTableSchemaCache::Insert(TGuid schemaId, TTableSchemaPtr schema)
{
    auto cachedSchema = New<TCachedTableSchema>(schemaId, std::move(schema));
    TryInsert(std::move(cachedSchema));
}

i64 TTableSchemaCache::GetWeight(const TCachedTableSchemaPtr& value) const
{
    return value->GetWeight();
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
