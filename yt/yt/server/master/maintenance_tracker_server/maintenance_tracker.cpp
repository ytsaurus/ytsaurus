#include "maintenance_tracker.h"

#include "private.h"
#include "cluster_proxy_node.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt/server/master/cypress_server/node_detail.h>
#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/maintenance_tracker_server/proto/maintenance_tracker.pb.h>

#include <yt/yt/server/master/node_tracker_server/host.h>
#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/server/master/security_server/security_manager.h>

namespace NYT::NMaintenanceTrackerServer {

using namespace NCellMaster;
using namespace NHydra;
using namespace NNodeTrackerServer;
using namespace NObjectServer;
using namespace NProto;
using namespace NYTree;

using NApi::ValidateMaintenanceComment;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = MaintenanceTrackerLogger;

////////////////////////////////////////////////////////////////////////////////

class TMaintenanceTracker
    : public IMaintenanceTracker
    , public TMasterAutomatonPart
{
public:
    using TMaintenanceIdList = TCompactVector<TMaintenanceId, TypicalMaintenanceRequestCount>;

    explicit TMaintenanceTracker(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::MaintenanceTracker)
    {
        RegisterMethod(BIND(&TMaintenanceTracker::HydraReplicateMaintenanceRequestCreation, Unretained(this)));
        RegisterMethod(BIND(&TMaintenanceTracker::HydraReplicateMaintenanceRequestRemoval, Unretained(this)));
    }

    TMaintenanceIdPerTarget AddMaintenance(
        EMaintenanceComponent component,
        const TString& address,
        EMaintenanceType type,
        const TString& comment,
        std::optional<NCypressServer::TNodeId> componentRegistryId) override
    {
        MaybeVerifyPrimaryMasterMutation(component);
        ValidateMaintenanceComment(comment);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto user = securityManager->GetAuthenticatedUser()->GetName();

        constexpr int TypicalNodePerHostCount = TEnumTraits<ENodeFlavor>::GetDomainSize();
        TCompactVector<
            std::tuple<TMaintenanceId, EMaintenanceComponent, TString>,
            TypicalNodePerHostCount> targets;

        if (component == EMaintenanceComponent::Host) {
            const auto& nodeTracker = Bootstrap_->GetNodeTracker();
            auto* host = nodeTracker->GetHostByNameOrThrow(address);
            ValidatePermission(host, EPermission::Read);

            for (auto* node : host->Nodes()) {
                ValidatePermission(node, EPermission::Write);
            }

            for (auto* node : host->Nodes()) {
                targets.emplace_back(
                    GenerateMaintenanceId(node),
                    EMaintenanceComponent::ClusterNode,
                    node->GetDefaultAddress());
            }
        } else {
            auto* target = GetComponentOrThrow(component, address, componentRegistryId);
            ValidatePermission(target->AsObject(), EPermission::Write);

            targets.emplace_back(
                GenerateMaintenanceId(target),
                component,
                address);
        }

        for (const auto& [id, component, address] : targets) {
            DoAddMaintenance(component, address, id, user, type, comment, componentRegistryId);
        }

        TMaintenanceIdPerTarget result;
        result.reserve(targets.size());
        for (const auto& [id, component, target] : targets) {
            result[target] = id;
        }
        return result;
    }

    TMaintenanceCountsPerTarget RemoveMaintenance(
        EMaintenanceComponent component,
        const TString& address,
        const std::optional<TCompactSet<TMaintenanceId, TypicalMaintenanceRequestCount>> ids,
        std::optional<TStringBuf> user,
        std::optional<EMaintenanceType> type,
        std::optional<NCypressServer::TNodeId> componentRegistryId) override
    {
        MaybeVerifyPrimaryMasterMutation(component);

        TCompactVector<std::tuple<EMaintenanceComponent, TString, TNontemplateMaintenanceTargetBase*>, 1> targets;

        if (component == EMaintenanceComponent::Host) {
            const auto& nodeTracker = Bootstrap_->GetNodeTracker();
            auto* host = nodeTracker->GetHostByNameOrThrow(address);
            ValidatePermission(host, EPermission::Read);

            for (auto* node : host->Nodes()) {
                ValidatePermission(node, EPermission::Write);
            }

            targets.reserve(host->Nodes().size());
            for (auto* node : host->Nodes()) {
                targets.emplace_back(
                    EMaintenanceComponent::ClusterNode,
                    node->GetDefaultAddress(),
                    node);
            }
        } else {
            auto* target = GetComponentOrThrow(component, address, componentRegistryId);
            ValidatePermission(target->AsObject(), EPermission::Write);
            targets.emplace_back(component, address, target);
        }

        TMaintenanceCountsPerTarget result;
        result.reserve(targets.size());
        for (const auto& [component, address, target] : targets) {
            const auto& maintenanceRequests = target->MaintenanceRequests();
            auto& resultPerTarget = result[address];

            TMaintenanceIdList filteredIds;
            if (ids) {
                filteredIds.reserve(std::min(ids->size(), maintenanceRequests.size()));
            } else {
                filteredIds.reserve(maintenanceRequests.size());
            }

            for (const auto& [id, request] : maintenanceRequests) {
                if ((!ids || ids->contains(id)) &&
                    (!user || user == request.User) &&
                    (!type || type == request.Type))
                {
                    filteredIds.push_back(id);
                    ++resultPerTarget[request.Type];
                }
            }

            Sort(filteredIds);
            DoRemoveMaintenances(component, address, filteredIds, componentRegistryId);
        }

        return result;
    }

private:
    void VerifyMaintenanceReplication(EMaintenanceComponent component)
    {
        YT_VERIFY(component == EMaintenanceComponent::ClusterNode);
        YT_VERIFY(HasMutationContext());

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsSecondaryMaster());
    }

    void HydraReplicateMaintenanceRequestCreation(TReqReplicateMaintenanceRequestCreation* request)
    {
        auto component = CheckedEnumCast<EMaintenanceComponent>(request->component());
        VerifyMaintenanceReplication(component);

        DoAddMaintenance(
            component,
            request->address(),
            FromProto<TMaintenanceId>(request->id()),
            request->user(),
            CheckedEnumCast<EMaintenanceType>(request->type()),
            request->comment(),
            /*componentRegistryId*/ std::nullopt);
    }

    void HydraReplicateMaintenanceRequestRemoval(TReqReplicateMaintenanceRequestRemoval* request)
    {
        auto component = CheckedEnumCast<EMaintenanceComponent>(request->component());
        VerifyMaintenanceReplication(component);

        auto ids = FromProto<TMaintenanceIdList>(request->ids());
        YT_ASSERT(std::is_sorted(ids.begin(), ids.end()));

        DoRemoveMaintenances(
            CheckedEnumCast<EMaintenanceComponent>(request->component()),
            request->address(),
            ids,
            /*componentRegistryId*/ std::nullopt);
    }

    void DoAddMaintenance(
        EMaintenanceComponent component,
        const TString& address,
        TMaintenanceId id,
        const TString& user,
        EMaintenanceType type,
        const TString& comment,
        std::optional<NCypressServer::TNodeId> componentMapNodeId)
    {
        YT_VERIFY(HasMutationContext());

        auto* target = FindComponentOrAlert(component, address, componentMapNodeId);
        if (!target) {
            return;
        }

        auto timestamp = TryGetCurrentMutationContext()->GetTimestamp();
        TMaintenanceRequest maintenanceRequest {
            .User = user,
            .Type = type,
            .Comment = comment,
            .Timestamp = timestamp,
        };
        if (target->AddMaintenance(id, std::move(maintenanceRequest))) {
            OnMaintenanceUpdated(component, type, target->AsObject());
        }

        YT_LOG_DEBUG(
            "Maintenance request added (Component: %v, Address: %v, Id: %v, User: %v, Type: %v, Comment: %v)",
            component,
            address,
            id,
            user,
            type,
            comment);

        if (!IsReplicationToSecondaryMastersNeeded(component)) {
            return;
        }

        TReqReplicateMaintenanceRequestCreation mutationRequest;
        mutationRequest.set_component(ToProto<i32>(component));
        mutationRequest.set_address(address);
        ToProto(mutationRequest.mutable_id(), id);
        mutationRequest.set_user(user);
        mutationRequest.set_type(ToProto<i32>(type));
        mutationRequest.set_comment(comment);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->PostToSecondaryMasters(mutationRequest);
    }

    void DoRemoveMaintenances(
        EMaintenanceComponent component,
        const TString& address,
        const TMaintenanceIdList& ids,
        std::optional<NCypressServer::TNodeId> componentRegistryId)
    {
        YT_VERIFY(HasMutationContext());

        auto* target = FindComponentOrAlert(component, address, componentRegistryId);
        if (!target) {
            return;
        }

        for (auto id : ids) {
            if (!target->MaintenanceRequests().contains(id)) {
                YT_LOG_ALERT("Cannot remove maintenance request (Component: %v, Address: %v, id: %v)",
                    component,
                    address,
                    id);
                continue;
            }
            if (auto type = target->RemoveMaintenance(id)) {
                OnMaintenanceUpdated(component, *type, target->AsObject());
            }
        }

        YT_LOG_DEBUG(
            "Maintenance requests removed (Component: %v, Address: %v, Ids: %v)",
            component,
            address,
            ids);

        if (ids.empty() || !IsReplicationToSecondaryMastersNeeded(component)) {
            return;
        }

        TReqReplicateMaintenanceRequestRemoval mutationRequest;
        mutationRequest.set_component(ToProto<i32>(component));
        mutationRequest.set_address(address);

        ToProto(mutationRequest.mutable_ids(), ids);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->PostToSecondaryMasters(mutationRequest);
    }

    bool IsReplicationToSecondaryMastersNeeded(EMaintenanceComponent component) const
    {
        YT_VERIFY(component != EMaintenanceComponent::Host);

        if (component != EMaintenanceComponent::ClusterNode) {
            return false;
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        return multicellManager->IsPrimaryMaster() && multicellManager->IsMulticell();
    }

    void ValidatePermission(NObjectServer::TObject* object, EPermission permission)
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidatePermission(object, permission);
    }

    TErrorOr<TNontemplateMaintenanceTargetBase*> DoFindComponent(
        EMaintenanceComponent component,
        const TString& address,
        std::optional<NCypressServer::TNodeId> componentRegistryId)
    {
        switch (component) {
            case NYT::NApi::EMaintenanceComponent::ClusterNode: {
                const auto& nodeTracker = Bootstrap_->GetNodeTracker();
                auto* node = nodeTracker->FindNodeByAddress(address);
                if (!IsObjectAlive(node)) {
                    return TError("No such node %Qv", address)
                        << TErrorAttribute("address", address);
                }

                return node;
            }
            case EMaintenanceComponent::HttpProxy: [[fallthrough]];
            case EMaintenanceComponent::RpcProxy: {
                if (!componentRegistryId) {
                    return TError(
                        "For RPC and HTTP proxies \"component_registry_id\" must be specified; "
                        "this request probably were made by an obsolete version of ytlib");
                }

                const auto& cypressManager = Bootstrap_->GetCypressManager();
                auto* targetMapNode = cypressManager->FindNode(NCypressServer::TVersionedNodeId(*componentRegistryId));
                if (!targetMapNode) {
                    return TError("%Qv not found",
                        component == EMaintenanceComponent::HttpProxy
                            ? NApi::HttpProxiesPath
                            : NApi::RpcProxiesPath);
                }

                const auto& targetMap = targetMapNode->As<NCypressServer::TCypressMapNode>()->KeyToChild();
                auto it = targetMap.find(address);
                if (it == targetMap.end()) {
                    return TError("No such proxy %Qv", address)
                        << TErrorAttribute("address", address);
                }

                if (it->second->GetType() != EObjectType::ClusterProxyNode) {
                    return TError("Proxy has to be represented by %Qlv instead of %Qlv to be able store maintenance requests",
                        EObjectType::ClusterProxyNode,
                        EObjectType::MapNode)
                        << TErrorAttribute("address", address);
                }

                return it->second->As<TClusterProxyNode>();
            }
            default:
                return TError("Maintenance component %Qlv is not supported", component)
                    << TErrorAttribute("component", component);
        }
    }

    TNontemplateMaintenanceTargetBase* GetComponentOrThrow(
        EMaintenanceComponent component,
        const TString& address,
        std::optional<NCypressServer::TNodeId> componentRegistryId)
    {
        YT_VERIFY(component != EMaintenanceComponent::Host);

        return DoFindComponent(component, address, componentRegistryId)
            .ValueOrThrow();
    }

    static TStringBuf FormatMaintenanceComponent(EMaintenanceComponent component) noexcept
    {
        switch (component) {
            case EMaintenanceComponent::ClusterNode:
                return "cluster node";
            case EMaintenanceComponent::HttpProxy:
                return  "HTTP proxy";
            case EMaintenanceComponent::RpcProxy:
                return "RPC proxy";
            default:
                YT_ABORT();
        }
    }

    TNontemplateMaintenanceTargetBase* FindComponentOrAlert(
        EMaintenanceComponent component,
        const TString& address,
        std::optional<NCypressServer::TNodeId> componentRegistryId)
    {
        YT_ASSERT(component != EMaintenanceComponent::Host);

        if (component != EMaintenanceComponent::ClusterNode &&
            component != EMaintenanceComponent::HttpProxy &&
            component != EMaintenanceComponent::RpcProxy)
        {
            YT_LOG_ALERT("Maintenance component is not supported yet (Component: %v)",
                component);
            return nullptr;
        }

        auto errorOrComponent = DoFindComponent(component, address, componentRegistryId);
        if (!errorOrComponent.IsOK()) {
            YT_LOG_ALERT("No such %v (Address: %v)",
                FormatMaintenanceComponent(component),
                address);
            return nullptr;
        }

        return errorOrComponent.Value();
    }

    void MaybeVerifyPrimaryMasterMutation(EMaintenanceComponent component)
    {
        YT_VERIFY(HasMutationContext());

        if (component == EMaintenanceComponent::Host || component == EMaintenanceComponent::ClusterNode) {
            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            YT_VERIFY(multicellManager->IsPrimaryMaster());
        }
    }

    static TMaintenanceId GenerateMaintenanceId(const TNontemplateMaintenanceTargetBase* target)
    {
        YT_VERIFY(NHydra::HasMutationContext());

        const auto& generator = NHydra::GetCurrentMutationContext()->RandomGenerator();
        TMaintenanceId id;
        do {
            id = TMaintenanceId(generator->Generate<ui64>(), generator->Generate<ui64>());
        } while (target->MaintenanceRequests().contains(id) || IsBuiltinMaintenanceId(id));

        return id;
    }

    void OnMaintenanceUpdated(
        EMaintenanceComponent component,
        EMaintenanceType type,
        TObject* target)
    {
        switch (component) {
            case EMaintenanceComponent::ClusterNode: {
                const auto& nodeTracker = Bootstrap_->GetNodeTracker();
                nodeTracker->OnNodeMaintenanceUpdated(static_cast<TNode*>(target), type);
                break;
            }
            default:
                YT_VERIFY(component != EMaintenanceComponent::Host);
                break;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IMaintenanceTrackerPtr CreateMaintenanceTracker(TBootstrap* bootstrap)
{
    return New<TMaintenanceTracker>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMaintenanceTrackerServer
