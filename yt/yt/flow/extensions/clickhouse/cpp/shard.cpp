#include "shard.h"

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <library/cpp/yt/string/format.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

#include <algorithm>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

void AppendLengthPrefixed(TStringBuilder* builder, const std::string& value)
{
    builder->AppendFormat("%v:%v,", value.size(), value);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool IsValidShardName(const std::string& name)
{
    if (name.empty()) {
        return false;
    }
    return std::all_of(name.begin(), name.end(), [] (char c) {
        return (c >= 'a' && c <= 'z') ||
            (c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9') ||
            c == '_' ||
            c == '-';
    });
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TClickHouseShard> ResolveShards(const TCommonClickHouseSinkParameters& parameters)
{
    if (parameters.ShardHosts.empty()) {
        return {TClickHouseShard{
            .Hosts = parameters.Hosts.empty()
                ? std::vector<std::string>{parameters.Host}
                : parameters.Hosts,
            .Port = parameters.Port,
            .Database = parameters.Database,
            .Table = parameters.Table,
        }};
    }

    std::vector<TClickHouseShard> shards;
    shards.reserve(parameters.ShardHosts.size());
    for (const auto& [name, hosts] : parameters.ShardHosts) {
        shards.push_back(TClickHouseShard{
            .Name = name,
            .Hosts = hosts,
            .Port = parameters.Port,
            .Database = parameters.Database,
            .Table = parameters.Table,
            .DedupTokenSuffix = ":" + name,
            .NameFingerprint = FarmFingerprint(TStringBuf(name)),
        });
    }
    std::sort(shards.begin(), shards.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs.Name < rhs.Name;
    });
    return shards;
}

void ValidateShardingKeyColumns(
    const std::vector<std::string>& shardingKeyColumns,
    const std::vector<NTableClient::TTableSchemaPtr>& streamSchemas)
{
    for (const auto& schema : streamSchemas) {
        for (const auto& name : shardingKeyColumns) {
            if (!schema->FindColumn(name)) {
                THROW_ERROR_EXCEPTION("Sharding key column %Qv is not produced by the input stream",
                    name);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TClickHouseShardRouter::TClickHouseShardRouter(
    std::vector<TClickHouseShard> shards,
    std::vector<std::string> shardingKeyColumns,
    std::vector<NTableClient::TTableSchemaPtr> streamSchemas)
    : Shards_(std::move(shards))
    , ShardingKeyColumns_(std::move(shardingKeyColumns))
    , StreamSchemas_(std::move(streamSchemas))
    , KeyColumnIndexes_([&] {
        ValidateShardingKeyColumns(ShardingKeyColumns_, StreamSchemas_);
        std::vector<TKeyColumnIndexes> result;
        result.reserve(StreamSchemas_.size());
        for (const auto& schema : StreamSchemas_) {
            TKeyColumnIndexes indexes;
            indexes.reserve(ShardingKeyColumns_.size());
            for (const auto& name : ShardingKeyColumns_) {
                indexes.push_back(schema->GetColumnIndexOrThrow(name));
            }
            result.push_back(std::move(indexes));
        }
        return result;
    }())
    , SchemaIndexes_([&] {
        THashMap<const NTableClient::TTableSchema*, size_t> result;
        for (size_t index = 0; index < StreamSchemas_.size(); ++index) {
            result.emplace(StreamSchemas_[index].Get(), index);
        }
        return result;
    }())
{
    YT_VERIFY(!Shards_.empty());
}

const std::vector<TClickHouseShard>& TClickHouseShardRouter::GetShards() const
{
    return Shards_;
}

const TClickHouseShardRouter::TKeyColumnIndexes&
TClickHouseShardRouter::GetKeyColumnIndexes(const NTableClient::TTableSchemaPtr& schema) const
{
    auto it = SchemaIndexes_.find(schema.Get());
    if (it != SchemaIndexes_.end()) {
        return KeyColumnIndexes_[it->second];
    }
    auto guard = Guard(CachedSchemaIndexesLock_);
    auto cachedIt = CachedSchemaIndexes_.find(schema.Get());
    if (cachedIt != CachedSchemaIndexes_.end()) {
        return KeyColumnIndexes_[cachedIt->second];
    }
    for (size_t index = 0; index < StreamSchemas_.size(); ++index) {
        if (*StreamSchemas_[index] == *schema) {
            CachedSchemas_.push_back(schema);
            CachedSchemaIndexes_.emplace(schema.Get(), index);
            return KeyColumnIndexes_[index];
        }
    }
    THROW_ERROR_EXCEPTION("ClickHouse routing received an unvalidated input schema");
}

TFingerprint TClickHouseShardRouter::ComputeKey(const TOutputMessageConstPtr& message) const
{
    if (ShardingKeyColumns_.empty()) {
        return FarmFingerprint(TStringBuf(message->MessageId.Underlying()));
    }
    const auto& indexes = GetKeyColumnIndexes(message->PayloadSchema);
    TCompactVector<NTableClient::TUnversionedValue, 4> values;
    values.reserve(indexes.size());
    for (int index : indexes) {
        values.push_back(GetColumn(*message, index));
    }
    return NTableClient::GetFarmFingerprint(
        NTableClient::TUnversionedValueRange(values.data(), values.size()));
}

int TClickHouseShardRouter::SelectShard(const TOutputMessageConstPtr& message) const
{
    if (Shards_.size() == 1) {
        return 0;
    }

    auto key = ComputeKey(message);
    int bestIndex = 0;
    auto bestScore = FarmFingerprint(key, Shards_[0].NameFingerprint);
    for (int index = 1; index < std::ssize(Shards_); ++index) {
        auto score = FarmFingerprint(key, Shards_[index].NameFingerprint);
        // Keeping the first maximum breaks ties towards the lexicographically smaller name,
        // because #ResolveShards sorted the shards by name.
        if (score > bestScore) {
            bestScore = score;
            bestIndex = index;
        }
    }
    return bestIndex;
}

////////////////////////////////////////////////////////////////////////////////

std::string BuildShardDedupToken(
    const std::string& batchDedupToken,
    const TClickHouseShard& shard)
{
    if (!shard.DedupTokenSuffix) {
        return batchDedupToken;
    }
    return batchDedupToken + *shard.DedupTokenSuffix;
}

std::string BuildShardTopologyFingerprint(
    const std::vector<TClickHouseShard>& shards,
    const std::vector<std::string>& shardingKeyColumns)
{
    if (shards.size() == 1 && shards.front().Name.empty()) {
        return std::string(UnshardedTopologyFingerprint);
    }
    // Routing depends on the key columns and their order just as much as on the shard set,
    // so a change there must trip the drain guard too. Every component is length-prefixed:
    // an unprefixed separator could let one topology's serialization alias another's.
    TStringBuilder builder;
    for (const auto& column : shardingKeyColumns) {
        AppendLengthPrefixed(&builder, column);
    }
    builder.AppendChar('|');
    for (const auto& shard : shards) {
        AppendLengthPrefixed(&builder, shard.Name);
    }
    return Format("%x", FarmFingerprint(TStringBuf(builder.Flush())));
}

std::string BuildShardTargetIdentityFingerprint(
    const TClickHouseShard& shard,
    const std::optional<TClickHouseReplicationIdentity>& replicationIdentity)
{
    TStringBuilder builder;
    if (replicationIdentity) {
        builder.AppendString("replicated|");
        AppendLengthPrefixed(&builder, replicationIdentity->ZookeeperName);
        AppendLengthPrefixed(&builder, replicationIdentity->ZookeeperPath);
    } else {
        builder.AppendString("endpoint|");
        AppendLengthPrefixed(&builder, shard.Database);
        AppendLengthPrefixed(&builder, shard.Table);
        builder.AppendFormat("%v|", shard.Port);
        auto hosts = shard.Hosts;
        std::sort(hosts.begin(), hosts.end());
        for (const auto& host : hosts) {
            AppendLengthPrefixed(&builder, host);
        }
    }
    return Format("%x", FarmFingerprint(TStringBuf(builder.Flush())));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
