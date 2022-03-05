#include "helpers.h"

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/queue_client/helpers.h>

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NQueueClient;
using namespace NTableServer;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TFuture<TYsonString> GetQueueAgentAttributeAsync(
    TBootstrap* bootstrap,
    const TTableNode* table,
    const TYPath& path,
    TInternedAttributeKey key)
{
    const auto& connection = bootstrap->GetClusterConnection();
    const auto& clusterName = connection->GetConfig()->ClusterName;
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

    auto queueAgentObjectService = CreateQueueAgentYPathService(
        connection->GetQueueAgentChannelOrThrow(table->GetQueueAgentStage()),
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
