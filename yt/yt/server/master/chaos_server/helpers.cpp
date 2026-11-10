#include "helpers.h"

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chaos_client/chaos_node_service_proxy.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/options.h>
#include <yt/yt/client/api/table_client.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NChaosServer {

using namespace NApi;
using namespace NChaosClient;
using namespace NServer;
using namespace NTransactionClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

TFuture<std::vector<TReplicationCardId>> GetCollocatedReplicationCards(
    TReplicationCardCollocationId collocationId,
    TReplicationCardId replicationCardId,
    const NNative::IConnectionPtr& connection)
{
    auto proxy = TChaosNodeServiceProxy(connection->GetChaosChannelByObjectIdOrThrow(replicationCardId));
    proxy.SetDefaultTimeout(connection->GetConfig()->DefaultChaosNodeServiceTimeout);
    auto req = proxy.GetReplicationCardCollocation();
    ToProto(req->mutable_replication_card_collocation_id(), collocationId);
    return req->Invoke()
        .AsUnique().Apply(BIND([] (TChaosNodeServiceProxy::TErrorOrRspGetReplicationCardCollocationPtr&& result) {
            if (!result.IsOK()) {
                return TErrorOr<std::vector<TReplicationCardId>>(TError(result));
            }

            return TErrorOr(
                FromProto<std::vector<TReplicationCardId>>(result.Value()->replication_card_ids()));
        }));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TTimestamp GetLastEraTimestamp(const TReplicationCardPtr& card)
{
    for (const auto& replica : card->Replicas) {
        for (const auto& historyItem : replica.second.History) {
            if (historyItem.Era == card->Era) {
                return historyItem.Timestamp;
            }
        }
    }

    return NullTimestamp;
}

////////////////////////////////////////////////////////////////////////////////

TErrorOr<int> GetMinimalTabletCount(std::vector<TErrorOr<int>> tabletCounts)
{
    if (tabletCounts.empty()) {
        return 0;
    }

    std::erase_if(
        tabletCounts,
        [] (const auto& tabletCount) {
            return !tabletCount.IsOK();
        });

    auto minElementIt = std::min_element(
        tabletCounts.begin(),
        tabletCounts.end(),
        [] (const auto& a, const auto& b) {
            return a.Value() < b.Value();
        });

    if (minElementIt == tabletCounts.end()) {
        return TError("Failed to get tablet count from any replica");
    }

    return minElementIt->Value();
}

TFuture<TReplicationCardPtr> GetReplicationCard(
    const NNative::IConnectionPtr& connection,
    TReplicationCardId replicationCardId,
    const TReplicationCardFetchOptions& options,
    bool bypassCache)
{
    TGetReplicationCardOptions getCardOptions;
    static_cast<TReplicationCardFetchOptions&>(getCardOptions) = options;
    getCardOptions.BypassCache = bypassCache;

    auto clientOptions = TClientOptions::FromAuthenticationIdentity(NRpc::GetCurrentAuthenticationIdentity());
    auto client = connection->CreateClient(clientOptions);

    return client->GetReplicationCard(replicationCardId, getCardOptions)
        .Apply(BIND([client] (const TReplicationCardPtr& card) {
            return card;
        }));
}

TFuture<TYsonString> GetReplicationCardAttribute(
    const NNative::IConnectionPtr& connection,
    TReplicationCardId replicationCardId,
    TInternedAttributeKey key)
{
    switch (key) {
        case EInternedAttributeKey::Era:
            return GetReplicationCard(connection, replicationCardId)
                .Apply(BIND([] (const TReplicationCardPtr& card) {
                    return BuildYsonStringFluently()
                        .Value(card->Era);
                }));

        case EInternedAttributeKey::CoordinatorCellIds:
            return GetReplicationCard(connection, replicationCardId, {.IncludeCoordinators = true})
                .Apply(BIND([] (const TReplicationCardPtr& card) {
                    return BuildYsonStringFluently()
                        .Value(card->CoordinatorCellIds);
                }));

        case EInternedAttributeKey::Replicas: {
            static constexpr auto Options = TReplicationCardFetchOptions{
                .IncludeProgress = true,
                .IncludeHistory = true,
                .IncludeReplicatedTableOptions = true,
            };

            return GetReplicationCard(connection, replicationCardId, Options)
                .Apply(BIND([] (const TReplicationCardPtr& card) {
                    auto replicasLags = ComputeReplicasLag(card->Replicas);
                    auto lastEraTimestamp = GetLastEraTimestamp(card);
                    return BuildYsonStringFluently()
                        .DoMapFor(replicasLags, [&] (TFluentMap fluent, const auto& lagPair) {
                            const auto& [replicaId, replicaLag] = lagPair;
                            const auto& replicas = card->Replicas;
                            const auto& replica = replicas.find(replicaId)->second;
                            const auto& replicaHistory = replica.History;
                            auto minTimestamp = GetReplicationProgressMinTimestamp(replica.ReplicationProgress);
                            bool replicaReachedLastOwnEra =
                                (!replicaHistory.empty() && minTimestamp >= replicaHistory.back().Timestamp);
                            fluent
                                .Item(ToString(replicaId))
                                .BeginMap()
                                    .Item("cluster_name").Value(replica.ClusterName)
                                    .Item("replica_path").Value(replica.ReplicaPath)
                                    .Item("state").Value(replica.State)
                                    .Item("mode").Value(replica.Mode)
                                    .Item("content_type").Value(replica.ContentType)
                                    .Item("replication_lag_timestamp").Value(minTimestamp)
                                    .Item("replication_lag_time").Value(replicaLag)
                                    .Item("replicated_table_tracker_enabled").Value(replica.EnableReplicatedTableTracker)
                                    .Item("replica_reached_last_own_era").Value(replicaReachedLastOwnEra)
                                    .Item("replica_reached_last_global_era").Value(minTimestamp >= lastEraTimestamp)
                                .EndMap();
                        });
                }));
        }

        case EInternedAttributeKey::ReplicatedTableOptions:
            return GetReplicationCard(connection, replicationCardId, {.IncludeReplicatedTableOptions = true})
                .Apply(BIND([] (const TReplicationCardPtr& card) {
                    return BuildYsonStringFluently()
                        .Value(card->ReplicatedTableOptions);
                }));

        case EInternedAttributeKey::ReplicationCollocationId:
            return GetReplicationCard(connection, replicationCardId)
                .Apply(BIND([] (const TReplicationCardPtr& card) {
                    return BuildYsonStringFluently()
                        .Value(card->ReplicationCardCollocationId);
                }));

        case EInternedAttributeKey::CollocatedReplicationCardIds:
            return GetReplicationCard(connection, replicationCardId)
                .AsUnique().Apply(BIND([connection, replicationCardId] (TReplicationCardPtr&& card) {
                    if (card->ReplicationCardCollocationId.IsEmpty()) {
                        return MakeFuture(
                            BuildYsonStringFluently()
                                .BeginList()
                                .EndList());
                    }

                    return GetCollocatedReplicationCards(
                        card->ReplicationCardCollocationId,
                        replicationCardId,
                        connection)
                        .AsUnique().Apply(BIND([] (std::vector<TReplicationCardId>&& ids) {
                            return BuildYsonStringFluently()
                                .Value(ids);
                        }));
                }));

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
