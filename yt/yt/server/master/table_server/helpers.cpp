#include "helpers.h"

#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/tablet_server/chaos_helpers.h>

#include <yt/yt/server/master/chaos_server/helpers.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/ytlib/queue_client/helpers.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <library/cpp/yt/misc/range_helpers.h>

namespace NYT::NTableServer {

using namespace NApi;
using namespace NCellMaster;
using namespace NChaosClient;
using namespace NChaosServer;
using namespace NQueueClient;
using namespace NRpc;
using namespace NTableClient;
using namespace NTableServer;
using namespace NTabletServer;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

std::string GetEffectiveQueueAgentStage(
    TBootstrap* bootstrap,
    const std::optional<std::string>& queueAgentStage)
{
    return queueAgentStage.value_or(
        bootstrap->GetConfigManager()->GetConfig()->QueueAgentServer->DefaultQueueAgentStage);
}

TFuture<TYsonString> GetQueueAgentAttributeAsync(
    TBootstrap* bootstrap,
    const std::optional<std::string>& queueAgentStageOptional,
    const TYPath& path,
    TInternedAttributeKey key)
{
    const auto& connection = bootstrap->GetClusterConnection();

    const auto& currentClusterName = connection->GetStaticConfig()->ClusterName;
    if (!currentClusterName) {
        THROW_ERROR_EXCEPTION("Cluster name is not set in cluster connection config");
    }

    std::string objectKind;
    switch (key) {
        case EInternedAttributeKey::QueueStatus:
        case EInternedAttributeKey::QueuePartitions:
            objectKind = "queue";
            break;
        case EInternedAttributeKey::QueueConsumerStatus:
        case EInternedAttributeKey::QueueConsumerPartitions:
            objectKind = "consumer";
            break;
        case EInternedAttributeKey::QueueProducerStatus:
        case EInternedAttributeKey::QueueProducerPartitions:
            objectKind = "producer";
            break;
        default:
            YT_ABORT();
    }

    auto queueAgentStage = GetEffectiveQueueAgentStage(bootstrap, queueAgentStageOptional);

    auto findQueueAgentChannelFromCluster = [&] (const std::string& clusterName) -> IChannelPtr {
        // NB: Instead of using cluster connection from our bootstrap, we take it
        // from the cluster directory. This works as a poor man's dynamic cluster connection
        // allowing us to reconfigure queue agent stages without need to update master config.
        auto dynamicConnection = connection->GetClusterDirectory()->FindConnection(clusterName);
        if (!dynamicConnection) {
            return nullptr;
        }

        return dynamicConnection->FindQueueAgentChannel(queueAgentStage);
    };

    auto queueAgentChannel = findQueueAgentChannelFromCluster(*currentClusterName);

    if (!queueAgentChannel) {
        for (const auto& clusterName : connection->GetClusterDirectory()->GetClusterNames()) {
            if (clusterName == *currentClusterName) {
                continue;
            }
            queueAgentChannel = findQueueAgentChannelFromCluster(clusterName);
            if (queueAgentChannel) {
                break;
            }
        }
    }

    if (!queueAgentChannel) {
        THROW_ERROR_EXCEPTION("Queue agent stage %Qv is not found", queueAgentStage);
    }

    auto queueAgentObjectService = CreateQueueAgentYPathService(
        queueAgentChannel,
        *currentClusterName,
        objectKind,
        path);

    TYPath remoteKey;
    switch (key) {
        case EInternedAttributeKey::QueueStatus:
        case EInternedAttributeKey::QueueConsumerStatus:
        case EInternedAttributeKey::QueueProducerStatus:
            remoteKey = "/status";
            break;
        case EInternedAttributeKey::QueuePartitions:
        case EInternedAttributeKey::QueueConsumerPartitions:
        case EInternedAttributeKey::QueueProducerPartitions:
            remoteKey = "/partitions";
            break;
        default:
            YT_ABORT();
    }
    return AsyncYPathGet(queueAgentObjectService, remoteKey);
}

TYsonString GetReplicationLagAttribute(
    TReplicaId upstreamReplicaId,
    const TReplicationCardPtr& replicationCard,
    const std::vector<TLegacyKey>& pivotKeys,
    const std::vector<TTabletId>& tabletIds)
{
    const auto* selfReplica = replicationCard->FindReplica(upstreamReplicaId);
    if (!selfReplica) {
        return BuildYsonStringFluently().Entity();
    }

    auto scatteredSyncProgress = ScatterReplicationProgress(
        BuildMaxSyncProgress(replicationCard->Replicas),
        pivotKeys,
        MaxKey().Get());
    auto scatteredReplicaProgress = ScatterReplicationProgress(
        selfReplica->ReplicationProgress,
        pivotKeys,
        MaxKey().Get());

    return BuildYsonStringFluently()
        .DoListFor(
            Zip(scatteredSyncProgress, scatteredReplicaProgress, tabletIds),
            [selfReplica] (TFluentList fluent, const auto& args) {
                const auto& [tabletSyncProgress, tabletProgress, tabletId] = args;
                auto minTabletProgressTimestamp = GetReplicationProgressMinTimestamp(tabletProgress);

                const auto& lastHistoryItem = selfReplica->History.back();
                auto replicaMode = minTabletProgressTimestamp >= lastHistoryItem.Timestamp
                    ? lastHistoryItem.Mode
                    : ETableReplicaMode::Async;

                auto lagTime = (replicaMode == ETableReplicaMode::Async)
                    ? ComputeReplicationProgressLag(tabletSyncProgress, tabletProgress)
                    : TDuration::Zero();

                fluent
                    .Item()
                        .BeginMap()
                            .Item("tablet_id").Value(ToString(tabletId))
                            .Item("replication_lag_time").Value(lagTime)
                            .Item("replication_mode").Value(replicaMode)
                        .EndMap();
            });
}

TFuture<NYson::TYsonString> GetReplicationLagTimesAsync(
    const TTableNode& table,
    const NNative::IConnectionPtr& connection)
{
    const auto& replicationCardId = table.GetTrunkNode()->GetReplicationCardId();
    if (!replicationCardId) {
        return MakeFuture(BuildYsonStringFluently().Entity());
    }

    const auto& tablets = table.Tablets();
    int tabletsCount = std::ssize(tablets);
    if (tabletsCount == 0) {
        return MakeFuture(BuildYsonStringFluently()
            .BeginList()
            .EndList());
    }

    std::vector<TLegacyKey> pivotKeys;
    std::vector<TTabletId> tabletIds;
    std::vector<TLegacyOwningKey> buffer;
    pivotKeys.reserve(tabletsCount);
    tabletIds.reserve(tabletsCount);
    if (!table.IsSorted()) {
        // Will contain all pivots except the first one.
        buffer.reserve(tabletsCount - 1);
    }

    for (int index = 0; index < tabletsCount; ++index) {
        const auto* tablet = tablets[index]->As<TTablet>();
        pivotKeys.push_back(GetTabletReplicationProgressPivotKey(tablet, index, &buffer));
        tabletIds.push_back(tablet->GetId());
    }

    TReplicationCardFetchOptions replicationCardFetchOptions;
    replicationCardFetchOptions.IncludeProgress = true;
    replicationCardFetchOptions.IncludeHistory = true;

    return GetReplicationCard(
        connection,
        replicationCardId,
        replicationCardFetchOptions)
        .Apply(BIND([
                upstreamReplicaId = table.GetUpstreamReplicaId(),
                pivotKeys = std::move(pivotKeys),
                tabletIds = std::move(tabletIds),
                buffer = std::move(buffer)
            ] (const TReplicationCardPtr& replicationCard) {
                return GetReplicationLagAttribute(upstreamReplicaId, replicationCard, pivotKeys, tabletIds);
            }));
}

TSchemaUpdateEnabledFeatures GetSchemaUpdateEnabledFeatures(TDynamicClusterConfigPtr config)
{
    return TSchemaUpdateEnabledFeatures{
        .EnableStaticTableDropColumn = config->EnableTableColumnRenaming &&
            config->EnableStaticTableDropColumn,

        .EnableDynamicTableDropColumn = config->EnableTableColumnRenaming &&
            config->EnableDynamicTableColumnRenaming &&
            config->EnableStaticTableDropColumn &&
            config->EnableDynamicTableDropColumn,

        .EnableStaticTableStructFieldRenaming = config->EnableStructFieldRenaming &&
            config->EnableStaticTableStructFieldRenaming,
        .EnableDynamicTableStructFieldRenaming = config->EnableStructFieldRenaming &&
            config->EnableDynamicTableStructFieldRenaming,

        .EnableStaticTableStructFieldRemoval = config->EnableStructFieldRemoval &&
            config->EnableStaticTableStructFieldRemoval,
        .EnableDynamicTableStructFieldRemoval = config->EnableStructFieldRemoval &&
            config->EnableDynamicTableStructFieldRemoval,
    };
}

void RecomputeTabletStatistics(TTableNode* table)
{
    table->ResetTabletStatistics();

    for (auto tablet : table->Tablets()) {
        table->AccountTabletStatistics(tablet->GetTabletStatistics());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
