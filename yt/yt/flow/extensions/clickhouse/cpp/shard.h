#pragma once

#include "public.h"

#include "spec.h"

#include <yt/yt/flow/library/cpp/common/message.h>

#include <yt/yt/client/table_client/public.h>

#include <library/cpp/yt/farmhash/farm_hash.h>
#include <library/cpp/yt/threading/spin_lock.h>

#include <optional>
#include <string>
#include <vector>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TClickHouseShardRouterTestPeer;

////////////////////////////////////////////////////////////////////////////////

inline constexpr TStringBuf UnshardedTopologyFingerprint = "unsharded";

bool IsValidShardName(const std::string& name);

////////////////////////////////////////////////////////////////////////////////

struct TClickHouseReplicationIdentity
{
    std::string ZookeeperName;
    std::string ZookeeperPath;

    friend bool operator==(
        const TClickHouseReplicationIdentity&,
        const TClickHouseReplicationIdentity&) = default;
};

////////////////////////////////////////////////////////////////////////////////

struct TClickHouseShard
{
    std::string Name;

    std::vector<std::string> Hosts;
    ui16 Port = 0;

    std::string Database;
    std::string Table;

    // Absent for the unsharded forms, whose token must stay byte-identical to the one those
    // pipelines already emitted.
    std::optional<std::string> DedupTokenSuffix;

    TFingerprint NameFingerprint = 0;
};

//! Resolves the spec into shards sorted by |Name|. The order is canonical: routing and the
//! topology fingerprint must not depend on the unspecified |ShardHosts| iteration order.
std::vector<TClickHouseShard> ResolveShards(const TCommonClickHouseSinkParameters& parameters);

void ValidateShardingKeyColumns(
    const std::vector<std::string>& shardingKeyColumns,
    const std::vector<NTableClient::TTableSchemaPtr>& streamSchemas);

////////////////////////////////////////////////////////////////////////////////

class TClickHouseShardRouter
{
public:
    using TKeyColumnIndexes = std::vector<int>;

    TClickHouseShardRouter(
        std::vector<TClickHouseShard> shards,
        std::vector<std::string> shardingKeyColumns,
        std::vector<NTableClient::TTableSchemaPtr> streamSchemas);

    const std::vector<TClickHouseShard>& GetShards() const;

    int SelectShard(const TOutputMessageConstPtr& message) const;

private:
    friend class TClickHouseShardRouterTestPeer;

    const std::vector<TClickHouseShard> Shards_;
    const std::vector<std::string> ShardingKeyColumns_;
    const std::vector<NTableClient::TTableSchemaPtr> StreamSchemas_;
    const std::vector<TKeyColumnIndexes> KeyColumnIndexes_;
    const THashMap<const NTableClient::TTableSchema*, size_t> SchemaIndexes_;
    mutable YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, CachedSchemaIndexesLock_);
    mutable THashMap<const NTableClient::TTableSchema*, size_t> CachedSchemaIndexes_;
    mutable std::vector<NTableClient::TTableSchemaPtr> CachedSchemas_;

    const TKeyColumnIndexes& GetKeyColumnIndexes(
        const NTableClient::TTableSchemaPtr& schema) const;
    TFingerprint ComputeKey(const TOutputMessageConstPtr& message) const;
};

////////////////////////////////////////////////////////////////////////////////

std::string BuildShardDedupToken(
    const std::string& batchDedupToken,
    const TClickHouseShard& shard);

//! Fingerprint of everything routing depends on: the sorted shard names and the sharding key
//! columns in spec order. Shard hosts are deliberately excluded — routing never reads them,
//! so replacing a dead host must not trip the drain guard. The unsharded forms are stamped
//! with #UnshardedTopologyFingerprint.
std::string BuildShardTopologyFingerprint(
    const std::vector<TClickHouseShard>& shards,
    const std::vector<std::string>& shardingKeyColumns);

std::string BuildShardTargetIdentityFingerprint(
    const TClickHouseShard& shard,
    const std::optional<TClickHouseReplicationIdentity>& replicationIdentity);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
