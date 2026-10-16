#include "sync_replica.h"

#include <yt/yt/core/ytree/node.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

TReplicaLocation FindEnabledReplicaImpl(
    const NYTree::IMapNodePtr& replicas,
    const NYPath::TYPath& objectPath,
    std::optional<TStringBuf> requiredContentType,
    std::optional<TStringBuf> requiredMode)
{
    std::optional<std::pair<std::string, TReplicaLocation>> result;
    for (const auto& [replicaId, descriptor] : replicas->GetChildren()) {
        const auto& attributes = descriptor->AsMap();
        if (requiredContentType &&
            attributes->GetChildOrThrow("content_type")->AsString()->GetValue() != *requiredContentType)
        {
            continue;
        }
        if (requiredMode && attributes->GetChildOrThrow("mode")->AsString()->GetValue() != *requiredMode) {
            continue;
        }
        if (auto stateChild = attributes->FindChild("state");
            stateChild && stateChild->AsString()->GetValue() != "enabled")
        {
            continue;
        }
        if (!result || replicaId < result->first) {
            result = std::pair{
                replicaId,
                TReplicaLocation{
                    .ClusterName = std::string(attributes->GetChildOrThrow("cluster_name")->AsString()->GetValue()),
                    .Path = NYPath::TYPath(attributes->GetChildOrThrow("replica_path")->AsString()->GetValue()),
                },
            };
        }
    }

    if (result) {
        return std::move(result->second);
    }

    if (!requiredMode) {
        if (requiredContentType) {
            THROW_ERROR_EXCEPTION("No enabled %v replica found for %v", *requiredContentType, objectPath);
        }
        THROW_ERROR_EXCEPTION("No enabled replica found for %v", objectPath);
    }
    if (requiredContentType) {
        THROW_ERROR_EXCEPTION(
            "No enabled synchronous %v replica found for %v",
            *requiredContentType,
            objectPath);
    }
    THROW_ERROR_EXCEPTION("No enabled synchronous replica found for %v", objectPath);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TReplicaLocation FindEnabledReplica(
    const NYTree::IMapNodePtr& replicas,
    const NYPath::TYPath& objectPath,
    std::optional<TStringBuf> requiredContentType)
{
    return FindEnabledReplicaImpl(replicas, objectPath, requiredContentType, /*requiredMode*/ {});
}

TReplicaLocation FindEnabledSyncReplica(
    const NYTree::IMapNodePtr& replicas,
    const NYPath::TYPath& objectPath,
    std::optional<TStringBuf> requiredContentType)
{
    return FindEnabledReplicaImpl(replicas, objectPath, requiredContentType, "sync");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
