#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include <yt/yt/client/federated/client.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/queue_client/dynamic_state.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

template <class TObjectSnapshotPtr>
TQueueAgentClientDirectory::TClientContext TQueueAgentClientDirectory::GetDataReadContext(
    const TObjectSnapshotPtr& snapshot,
    bool onlyDataReplicas)
{
    const NQueueClient::TCrossClusterReference& object = snapshot->Row.Ref;

    if (!snapshot->Row.ObjectType) {
        THROW_ERROR_EXCEPTION("Cannot get client for object %Qv with unknown object type", object);
    }
    NCypressClient::EObjectType objectType = *snapshot->Row.ObjectType;

    const std::optional<NQueueClient::TReplicatedTableMappingTableRow>& replicatedTableMappingRow = snapshot->ReplicatedTableMappingRow;

    switch (objectType) {
        case NCypressClient::EObjectType::Table:
        case NCypressClient::EObjectType::ReplicatedTable:
            return {
                .Client = ClientDirectory_->GetClientOrThrow(object.Cluster),
                .Path = object.Path
            };
        case NCypressClient::EObjectType::ChaosReplicatedTable: {
            if (!replicatedTableMappingRow) {
                THROW_ERROR_EXCEPTION(
                    "Cannot get data read client for chaos replicated table %Qv without replica info",
                    object);
            }
            auto replicas = GetRelevantReplicas(
                *replicatedTableMappingRow,
                /*onlySyncReplicas*/ false,
                onlyDataReplicas,
                /*validatePaths*/ true);
            if (replicas.empty()) {
                THROW_ERROR_EXCEPTION("Cannot get data read client for chaos replicated table %Qv since there are no known replicas", object);
            }
            return {
                .Client = GetFederatedClient(replicas),
                .Path = replicas[0].GetPath(),
            };
        }
        default:
            THROW_ERROR_EXCEPTION("Cannot get data read client for object %Qv of type %Qlv", object, objectType);
    }
}

template <class TObjectSnapshotPtr>
TQueueAgentClientDirectory::TNativeClientContext TQueueAgentClientDirectory::GetNativeSyncClient(
    const TObjectSnapshotPtr& snapshot,
    bool onlyDataReplicas)
{
    const NQueueClient::TCrossClusterReference& object = snapshot->Row.Ref;

    if (!snapshot->Row.ObjectType) {
        THROW_ERROR_EXCEPTION("Cannot get client for object %Qv with unknown object type", object);
    }
    NCypressClient::EObjectType objectType = *snapshot->Row.ObjectType;

    const std::optional<NQueueClient::TReplicatedTableMappingTableRow>& replicatedTableMappingRow = snapshot->ReplicatedTableMappingRow;

    switch (objectType) {
        case NCypressClient::EObjectType::Table:
            return {
                .Client = ClientDirectory_->GetClientOrThrow(object.Cluster),
                .Path = object.Path,
            };
        case NCypressClient::EObjectType::ReplicatedTable:
        case NCypressClient::EObjectType::ChaosReplicatedTable: {
            if (!replicatedTableMappingRow) {
                THROW_ERROR_EXCEPTION(
                    "Cannot get data read client for [chaos] replicated table %Qv without replica info",
                    object);
            }

            auto syncReplicas = GetRelevantReplicas(
                *replicatedTableMappingRow,
                /*onlySyncReplicas*/ true,
                onlyDataReplicas,
                /*validatePaths*/ false);

            if (syncReplicas.empty()) {
                THROW_ERROR_EXCEPTION("Cannot get sync client for %Qv since there are no known sync replicas", object);
            }
            // TODO(achulkov2): Try to pick a cluster that is alive.
            return {
                .Client = ClientDirectory_->GetClientOrThrow(*syncReplicas[0].GetCluster()),
                .Path = syncReplicas[0].GetPath(),
            };
        }
        default:
            THROW_ERROR_EXCEPTION("Cannot get sync client for object %Qv of type %Qlv", object, objectType);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
std::optional<T> MinOrValue(const std::optional<T> lhs, const std::optional<T> rhs)
{
    if (lhs && rhs) {
        return std::min(lhs, rhs);
    } else if (!lhs) {
        return rhs;
    } else {
        return lhs;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
