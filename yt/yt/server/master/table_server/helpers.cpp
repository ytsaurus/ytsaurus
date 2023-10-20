#include "helpers.h"

#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/queue_client/helpers.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NQueueClient;
using namespace NTableClient;
using namespace NTableServer;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TString GetEffectiveQueueAgentStage(
    TBootstrap* bootstrap,
    const std::optional<TString>& queueAgentStage)
{
    return queueAgentStage.value_or(
        bootstrap->GetConfigManager()->GetConfig()->QueueAgentServer->DefaultQueueAgentStage);
}

TFuture<TYsonString> GetQueueAgentAttributeAsync(
    TBootstrap* bootstrap,
    const std::optional<TString>& queueAgentStageOptional,
    const TYPath& path,
    TInternedAttributeKey key)
{
    const auto& connection = bootstrap->GetClusterConnection();
    const auto& clusterName = connection->GetStaticConfig()->ClusterName;
    if (!clusterName) {
        THROW_ERROR_EXCEPTION("Cluster name is not set in cluster connection config");
    }

    TString objectKind;
    switch (key) {
        case EInternedAttributeKey::QueueStatus:
        case EInternedAttributeKey::QueuePartitions:
            objectKind = "queue";
            break;
        case EInternedAttributeKey::QueueConsumerStatus:
        case EInternedAttributeKey::QueueConsumerPartitions:
            objectKind = "consumer";
            break;
        default:
            YT_ABORT();
    }

    auto queueAgentStage = GetEffectiveQueueAgentStage(bootstrap, queueAgentStageOptional);

    // NB: instead of using cluster connection from our bootstrap, we take it
    // from the cluster directory. This works as a poor man's dynamic cluster connection
    // allowing us to reconfigure queue agent stages without need to update master config.
    auto dynamicConnection = connection->GetClusterDirectory()->GetConnectionOrThrow(*clusterName);

    auto queueAgentObjectService = CreateQueueAgentYPathService(
        dynamicConnection->GetQueueAgentChannelOrThrow(queueAgentStage),
        *clusterName,
        objectKind,
        path);

    TYPath remoteKey;
    switch (key) {
        case EInternedAttributeKey::QueueStatus:
        case EInternedAttributeKey::QueueConsumerStatus:
            remoteKey = "/status";
            break;
        case EInternedAttributeKey::QueuePartitions:
        case EInternedAttributeKey::QueueConsumerPartitions:
            remoteKey = "/partitions";
            break;
        default:
            YT_ABORT();
    }
    return AsyncYPathGet(queueAgentObjectService, remoteKey);
}

TSchemaUpdateEnabledFeatures GetSchemaUpdateEnabledFeatures(TDynamicClusterConfigPtr config)
{
    return TSchemaUpdateEnabledFeatures{
        config->EnableTableColumnRenaming && config->EnableStaticTableDropColumn,

        // TODO(orlovorlov) YT-16507 add && config->EnableDynamicTableColumnRenaming here when
        // review/3730137 is merged.
        config->EnableTableColumnRenaming && config->EnableStaticTableDropColumn &&
            config->EnableDynamicTableDropColumn
    };
}

void RecomputeTabletStatistics(TTableNode* table)
{
    table->ResetTabletStatistics();

    for (const auto* tablet : table->Tablets()) {
        table->AccountTabletStatistics(tablet->GetTabletStatistics());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
