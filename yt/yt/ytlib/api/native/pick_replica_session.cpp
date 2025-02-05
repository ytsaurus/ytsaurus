#include "pick_replica_session.h"
#include "config.h"
#include "connection.h"
#include "private.h"
#include "table_replica_synchronicity_cache.h"
#include "tablet_helpers.h"

#include <yt/yt/ytlib/chaos_client/banned_replica_tracker.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

namespace NYT::NApi::NNative {

using namespace NChaosClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NQueryClient;

using TClusterScoreMap = THashMap<std::string, TTimestamp>;

const std::string UpstreamReplicaIdAttributeName = "upstream_replica_id";

////////////////////////////////////////////////////////////////////////////////

template <class TCallback>
void TraverseQueryTables(NAst::TQuery* query, TCallback onTableDescriptor)
{
    if (query->WithIndex) {
        onTableDescriptor(*query->WithIndex);
    }

    Visit(query->FromClause,
        [&] (NAst::TTableDescriptor& table) {
            onTableDescriptor(table);
        },
        [&] (NAst::TQueryAstHeadPtr& subquery) {
            TraverseQueryTables(&subquery->Ast, onTableDescriptor);
        });

    for (auto& join : query->Joins) {
        if (auto* tableJoin = std::get_if<NAst::TJoin>(&join)) {
            onTableDescriptor(tableJoin->Table);
        }
    }
}

bool IsChaos(const TTableMountInfoPtr& tableInfo)
{
    return static_cast<bool>(tableInfo->ReplicationCardId);
}

std::vector<TFuture<TTableMountInfoPtr>> GetQueryTableInfos(
    NAst::TQuery* query,
    const ITableMountCachePtr& cache)
{
    std::vector<TFuture<TTableMountInfoPtr>> asyncTableInfos;

    TraverseQueryTables(query, [&] (NAst::TTableDescriptor& table) {
        asyncTableInfos.push_back(cache->GetTableInfo(table.Path));
    });

    return asyncTableInfos;
}

std::vector<NAst::TTableHintPtr> GetQueryTableHints(NAst::TQuery* query)
{
    std::vector<NAst::TTableHintPtr> hints;

    TraverseQueryTables(query, [&] (NAst::TTableDescriptor& table) {
        hints.push_back(table.Hint);
    });

    return hints;
}

std::vector<IBannedReplicaTrackerPtr> GetBannedReplicaTrackers(
    const IConnectionPtr& connection,
    TRange<TTableMountInfoPtr> tableInfos)
{
    std::vector<IBannedReplicaTrackerPtr> bannedReplicaTrackers(tableInfos.size());
    const auto& bannedReplicaTrackerCache = connection->GetBannedReplicaTrackerCache();

    for (int index = 0; index < std::ssize(tableInfos); ++index) {
        if (IsChaos(tableInfos[index])) {
            bannedReplicaTrackers[index] = bannedReplicaTrackerCache->GetTracker(tableInfos[index]->TableId);
        }
    }

    return bannedReplicaTrackers;
}

void PatchQuery(NAst::TQuery* query, const TReplicaSynchronicityList& replicas)
{
    int index = 0;
    TraverseQueryTables(query, [&] (NAst::TTableDescriptor& table) {
        table.Path = replicas[index++].ReplicaInfo->ReplicaPath;
    });
}

////////////////////////////////////////////////////////////////////////////////

TSelectRowsOptionsBase::TExpectedTableSchemas MakeChaosExpectedSchemas(
    TRange<TTableMountInfoPtr> mountInfos,
    TRange<TReplicaSynchronicity> replicaStatuses)
{
    YT_VERIFY(mountInfos.size() == replicaStatuses.size());

    TSelectRowsOptionsBase::TExpectedTableSchemas expectedSchemas;
    for (int index = 0; index < std::ssize(mountInfos); ++index) {
        if (IsChaos(mountInfos[index])) {
            expectedSchemas[replicaStatuses[index].ReplicaInfo->ReplicaPath] =
                mountInfos[index]->Schemas[ETableSchemaKind::Primary];
        }
    }

    return expectedSchemas;
}

bool CheckReplicated(
    TRange<TTableMountInfoPtr> tableInfos,
    EReplicaConsistency replicaConsistency)
{
    bool someReplicated = false;
    bool someNotReplicated = false;
    for (const auto& tableInfo : tableInfos) {
        if (tableInfo->IsReplicationLog()) {
            THROW_ERROR_EXCEPTION("Replication log table is not supported for this type of query");
        }
        if (tableInfo->IsReplicated() ||
            tableInfo->IsChaosReplicated() ||
            (IsChaos(tableInfo) && replicaConsistency == EReplicaConsistency::Sync))
        {
            someReplicated = true;
        } else {
            someNotReplicated = true;
        }
    }

    if (someReplicated && someNotReplicated) {
        THROW_ERROR_EXCEPTION("Query involves both replicated and non-replicated tables");
    }

    return someReplicated;
}

bool IsReplicaSyncEnough(
    bool requireSyncReplica,
    const TReplicaSynchronicity& replicaStatus,
    TTimestamp userTimestamp)
{
    return !requireSyncReplica ||
        replicaStatus.IsInSync ||
        IsTimestampInSync(userTimestamp, replicaStatus.MinReplicationTimestamp);
}

std::string PickRandomCluster(TRange<std::string> clusterNames)
{
    YT_VERIFY(!clusterNames.empty());
    return clusterNames[RandomNumber<size_t>(clusterNames.size())];
}

////////////////////////////////////////////////////////////////////////////////

class TPickReplicaSession
    : public IPickReplicaSession
{
public:
    TPickReplicaSession() = default;

    TPickReplicaSession(
        std::vector<TReplicaSynchronicityList> replicas,
        NAst::TQuery* query,
        const TSelectRowsOptionsBase& baseOptions,
        std::vector<TTableMountInfoPtr> tableInfos,
        std::vector<NAst::TTableHintPtr> tableHints,
        std::vector<IBannedReplicaTrackerPtr> bannedReplicaTrackers);

    TResult Execute(const IConnectionPtr& connection, TExecuteCallback callback) override;

    bool IsFallbackRequired() const override;

private:
    const bool IsFallbackRequired_ = false;
    const std::vector<TReplicaSynchronicityList> Replicas_;
    NAst::TQuery* const AstQuery_ = nullptr;
    const std::vector<TTableMountInfoPtr> TableInfos_;
    const std::vector<NAst::TTableHintPtr> TableHints_;
    const std::vector<IBannedReplicaTrackerPtr> BannedReplicaTrackers_;
    TSelectRowsOptionsBase BaseOptions_;

    // Returns a map without scores.
    TClusterScoreMap PickViableClusters() const;
    std::pair<std::string, TReplicaSynchronicityList> PickClusterAndReplicas(TClusterScoreMap viableClusters) const;
};

TPickReplicaSession::TPickReplicaSession(
    std::vector<TReplicaSynchronicityList> replicas,
    NAst::TQuery* query,
    const TSelectRowsOptionsBase& baseOptions,
    std::vector<TTableMountInfoPtr> tableInfos,
    std::vector<NAst::TTableHintPtr> tableHints,
    std::vector<IBannedReplicaTrackerPtr> bannedReplicaTrackers)
    : IsFallbackRequired_(true)
    , Replicas_(std::move(replicas))
    , AstQuery_(query)
    , TableInfos_(std::move(tableInfos))
    , TableHints_(std::move(tableHints))
    , BannedReplicaTrackers_(std::move(bannedReplicaTrackers))
    , BaseOptions_(baseOptions)
{ }

TPickReplicaSession::TResult TPickReplicaSession::Execute(
    const IConnectionPtr& connection,
    TExecuteCallback callback)
{
    const auto& Logger = connection->GetLogger();

    BaseOptions_.ReplicaConsistency = EReplicaConsistency::None;

    const auto replicaFallbackRetryCount = connection->GetConfig()->ReplicaFallbackRetryCount;
    THashMap<TTableReplicaId, TTableId> replicaIdToTableId;
    int retryCountLimit = 0;
    for (int index = 0; index < std::ssize(TableInfos_); ++index) {
        const auto& tableInfo = TableInfos_[index];
        if (IsChaos(tableInfo)) {
            retryCountLimit = std::max(retryCountLimit, replicaFallbackRetryCount);
            for (const auto& replica : Replicas_[index]) {
                replicaIdToTableId[replica.ReplicaInfo->ReplicaId] = tableInfo->TableId;
            }
        }
    }

    TError error;
    for (int retryCount = 0; retryCount <= retryCountLimit; ++retryCount) {
        try {
            auto [cluster, replicas] = PickClusterAndReplicas(PickViableClusters());

            YT_LOG_DEBUG("Fallback to replicas (Cluster: %v, Replicas: %v, Attempt: %v)",
                cluster,
                replicas,
                retryCount);

            PatchQuery(AstQuery_, replicas);

            auto queryString = NAst::FormatQuery(*AstQuery_);
            BaseOptions_.ExpectedTableSchemas = MakeChaosExpectedSchemas(TableInfos_, replicas);
            return callback(cluster, queryString, BaseOptions_);
        } catch (const TErrorException& ex) {

            YT_LOG_DEBUG(ex, "Fallback to replicas failed (Attempt: %v)", retryCount);

            error = ex.Error();
            if (auto schemaError = error.FindMatching(NTabletClient::EErrorCode::TableSchemaIncompatible)) {
                if (auto replicaId = schemaError->Attributes().Find<TTableReplicaId>(UpstreamReplicaIdAttributeName)) {
                    if (auto it = replicaIdToTableId.find(*replicaId)) {
                        auto bannedReplicaTracker = connection
                            ->GetBannedReplicaTrackerCache()
                            ->GetTracker(it->second);
                        bannedReplicaTracker->BanReplica(*replicaId, error.Truncate());
                    }
                }
            }
        }
    }

    error.ThrowOnError();

    YT_ABORT();
}

bool TPickReplicaSession::IsFallbackRequired() const
{
    return IsFallbackRequired_;
}

TClusterScoreMap TPickReplicaSession::PickViableClusters() const
{
    TClusterScoreMap viableClusters;

    auto updateViableClusters = [&, first=true] (const TClusterScoreMap& filter) mutable {
        if (first) {
            viableClusters = filter;
            first = false;
        } else {
            TClusterScoreMap intersection;
            for (auto& [cluster, _] : viableClusters) {
                if (filter.contains(cluster)) {
                    intersection[cluster] = {};
                }
            }
            viableClusters = std::move(intersection);
        }
    };

    auto getViableClusters = [&] (
        const TReplicaSynchronicityList& replicas,
        const IBannedReplicaTrackerPtr& bannedReplicaTracker,
        std::vector<TTableReplicaId>& bannedReplicaIds,
        bool requireSyncReplica)
    {
        TClusterScoreMap clusters;

        for (const auto& replica : replicas) {
            if (bannedReplicaTracker &&
                bannedReplicaTracker->IsReplicaBanned(replica.ReplicaInfo->ReplicaId))
            {
                bannedReplicaIds.push_back(replica.ReplicaInfo->ReplicaId);
                continue;
            }

            if (IsReplicaSyncEnough(requireSyncReplica, replica, BaseOptions_.Timestamp)) {
                clusters[replica.ReplicaInfo->ClusterName] = {};
            }
        }

        return clusters;
    };

    std::vector<TTableReplicaId> flatBannedReplicaIds;
    std::vector<TError> flatReplicaErrors;

    for (int index = 0; index < std::ssize(TableInfos_); ++index) {
        std::vector<TTableReplicaId> bannedReplicaIds;
        std::vector<TError> replicaErrors;

        auto tableViableClusters = getViableClusters(
            Replicas_[index],
            BannedReplicaTrackers_[index],
            bannedReplicaIds,
            TableHints_[index]->RequireSyncReplica);

        if (tableViableClusters.empty()) {
            for (auto id : bannedReplicaIds) {
                if (const auto& error = BannedReplicaTrackers_[index]->GetReplicaError(id);
                    !error.IsOK())
                {
                    replicaErrors.push_back(error);
                }
            }

            auto error = TError(
                NTabletClient::EErrorCode::NoInSyncReplicas,
                "No single cluster contains in-sync replicas for table %v",
                TableInfos_[index]->Path)
                << TErrorAttribute("banned_replicas", bannedReplicaIds);

            *error.MutableInnerErrors() = std::move(replicaErrors);

            THROW_ERROR error;
        }

        std::move(bannedReplicaIds.begin(), bannedReplicaIds.end(), std::back_inserter(flatBannedReplicaIds));
        std::move(replicaErrors.begin(), replicaErrors.end(), std::back_inserter(flatReplicaErrors));

        updateViableClusters(tableViableClusters);
    }

    if (viableClusters.empty()) {
        auto error = TError(
            NTabletClient::EErrorCode::NoInSyncReplicas,
            "No single cluster contains in-sync replicas for all involved tables %v",
            MakeFormattableView(TableInfos_, [] (TStringBuilderBase* builder, const TTableMountInfoPtr& tableInfo) {
                builder->AppendString(tableInfo->Path);
            }))
            << TErrorAttribute("banned_replicas", flatBannedReplicaIds);

        *error.MutableInnerErrors() = std::move(flatReplicaErrors);

        THROW_ERROR error;
    }

    return viableClusters;
}

std::pair<std::string, TReplicaSynchronicityList> TPickReplicaSession::PickClusterAndReplicas(
    TClusterScoreMap viableClusters) const
{
    if (viableClusters.empty()) {
        THROW_ERROR_EXCEPTION(NTabletClient::EErrorCode::NoInSyncReplicas, "Not a single viable cluster found");
    }

    for (int index = 0; index < std::ssize(TableInfos_); ++index) {
        if (!TableHints_[index]->RequireSyncReplica) {
            THashMap<std::string, TTimestamp> tableScores;
            for (const auto& replicas : Replicas_[index]) {
                if (!viableClusters.contains(replicas.ReplicaInfo->ClusterName)) {
                    continue;
                }
                auto& clusterScore = tableScores[replicas.ReplicaInfo->ClusterName];
                clusterScore = std::max(clusterScore, replicas.MinReplicationTimestamp);
            }

            for (const auto& [cluster, bestScore] : tableScores) {
                GetOrCrash(viableClusters, cluster) += bestScore;
            }
        }
    }

    auto bestScore = NullTimestamp;
    std::vector<std::string> topScoringClusters;
    for (const auto& [_, score] : viableClusters) {
        if (score > bestScore) {
            bestScore = score;
        }
    }
    for (const auto& [cluster, score] : viableClusters) {
        if (score == bestScore) {
            topScoringClusters.push_back(cluster);
        }
    }

    auto cluster = PickRandomCluster(topScoringClusters);

    TReplicaSynchronicityList replicas;
    for (int index = 0; index < std::ssize(Replicas_); ++index) {
        const auto requireSyncReplica = TableHints_[index]->RequireSyncReplica;
        const auto userTimestamp = BaseOptions_.Timestamp;

        auto isGoodReplica = [&] (const TReplicaSynchronicity& replica) {
            return IsReplicaSyncEnough(requireSyncReplica, replica, userTimestamp) &&
                replica.ReplicaInfo->ClusterName == cluster;
        };

        ui32 topScoringReplicaCount = 0;
        auto bestScore = NullTimestamp;
        for (const auto& replica : Replicas_[index] | std::ranges::views::filter(isGoodReplica)) {
            if (replica.MinReplicationTimestamp >= bestScore) {
                if (replica.MinReplicationTimestamp == bestScore) {
                    topScoringReplicaCount++;
                } else {
                    topScoringReplicaCount = 1;
                    bestScore = replica.MinReplicationTimestamp;
                }
            }
        }

        YT_VERIFY(topScoringReplicaCount > 0);

        int randomReplicaIndex = RandomNumber<ui32>(topScoringReplicaCount);
        int topScoringReplicaIndex = 0;
        for (const auto& replica : Replicas_[index] | std::ranges::views::filter(isGoodReplica)) {
            if (replica.MinReplicationTimestamp == bestScore &&
                (topScoringReplicaIndex++ == randomReplicaIndex))
            {
                replicas.push_back(replica);
                break;
            }
        }
    }

    return {std::move(cluster), std::move(replicas)};
}

////////////////////////////////////////////////////////////////////////////////

IPickReplicaSessionPtr CreatePickReplicaSession(
    NAst::TQuery* query,
    const IConnectionPtr& connection,
    const ITableMountCachePtr& mountCache,
    const TTableReplicaSynchronicityCachePtr& replicaSynchronicityCache,
    const TSelectRowsOptionsBase& options)
{
    const auto& Logger = connection->GetLogger();

    auto tableInfos = WaitForFast(AllSucceeded(GetQueryTableInfos(query, mountCache)))
        .ValueOrThrow();
    auto tableHints = GetQueryTableHints(query);
    auto bannedReplicaTrackers = GetBannedReplicaTrackers(connection, tableInfos);

    if (!CheckReplicated(tableInfos, options.ReplicaConsistency)) {
        return New<TPickReplicaSession>();
    }

    std::vector<TFuture<TReplicaSynchronicityList>> futureReplicas;
    futureReplicas.reserve(tableInfos.size());
    if (options.CachedSyncReplicasTimeout) {
        auto deadline = TInstant::Now() - *options.CachedSyncReplicasTimeout;
        for (const auto& table : tableInfos) {
            futureReplicas.push_back(replicaSynchronicityCache
                ->GetReplicaSynchronicities(connection, table, deadline, options));
        }
    } else {
        for (const auto& table : tableInfos) {
            futureReplicas.push_back(FetchReplicaSynchronicities(connection, table, options));
        }
    }

    auto replicas = WaitFor(AllSucceeded(std::move(futureReplicas)))
        .ValueOrThrow();

    YT_LOG_DEBUG("Fetched table replica synchronicities (TablePaths: %v, ReplicaSynchronicities: %v)",
        MakeFormattableView(tableInfos, [] (TStringBuilderBase* builder, const TTableMountInfoPtr& tableInfo) {
            builder->AppendString(tableInfo->Path);
        }),
        replicas);

    return New<TPickReplicaSession>(
        std::move(replicas),
        query,
        options,
        std::move(tableInfos),
        std::move(tableHints),
        std::move(bannedReplicaTrackers));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
