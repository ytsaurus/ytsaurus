#include "maintenance_tracker.h"

#include "private.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>

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

    TMaintenanceId AddMaintenance(
        EMaintenanceComponent component,
        const TString& address,
        EMaintenanceType type,
        const TString& comment) override
    {
        VerifyPrimaryMasterMutation();
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
            auto* target = GetComponentOrThrow(component, address);
            ValidatePermission(target->AsObject(), EPermission::Write);

            targets.emplace_back(
                GenerateMaintenanceId(target),
                component,
                address);
        }

        for (const auto& [id, component, address] : targets) {
            DoAddMaintenance(component, address, id, user, type, comment);
        }

        return targets.size() == 1 ? std::get<TMaintenanceId>(targets.front()) : TMaintenanceId{};
    }

    TMaintenanceCounts RemoveMaintenance(
        EMaintenanceComponent component,
        const TString& address,
        const std::optional<TCompactSet<TMaintenanceId, TypicalMaintenanceRequestCount>> ids,
        std::optional<TStringBuf> user,
        std::optional<EMaintenanceType> type) override
    {
        VerifyPrimaryMasterMutation();

        TCompactVector<std::tuple<EMaintenanceComponent, TString, TNontemplateMaintenanceTargetBase*>, 2> targets;

        if (component == EMaintenanceComponent::Host) {
            const auto& nodeTracker = Bootstrap_->GetNodeTracker();
            auto* host = nodeTracker->GetHostByNameOrThrow(address);
            ValidatePermission(host, EPermission::Read);

            for (auto* node : host->Nodes()) {
                ValidatePermission(node, EPermission::Write);
            }

            for (auto* node : host->Nodes()) {
                targets.emplace_back(
                    EMaintenanceComponent::ClusterNode,
                    node->GetDefaultAddress(),
                    node);
            }
        } else {
            auto* target = GetComponentOrThrow(component, address);
            ValidatePermission(target->AsObject(), EPermission::Write);
            targets.emplace_back(component, address, target);
        }

        TMaintenanceCounts result;
        for (const auto& [component, address, target] : targets) {
            const auto& maintenanceRequests = target->MaintenanceRequests();

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
                    ++result[request.Type];
                }
            }

            DoRemoveMaintenances(component, address, filteredIds);
        }

        return result;
    }

private:
    void HydraReplicateMaintenanceRequestCreation(TReqReplicateMaintenanceRequestCreation* request)
    {
        VerifySecondaryMasterMutation();

        DoAddMaintenance(
            CheckedEnumCast<EMaintenanceComponent>(request->component()),
            request->address(),
            FromProto<TMaintenanceId>(request->id()),
            request->user(),
            CheckedEnumCast<EMaintenanceType>(request->type()),
            request->comment());
    }

    void HydraReplicateMaintenanceRequestRemoval(TReqReplicateMaintenanceRequestRemoval* request)
    {
        VerifySecondaryMasterMutation();

        auto ids = FromProto<TCompactVector<TMaintenanceId, TypicalMaintenanceRequestCount>>(request->ids());
        DoRemoveMaintenances(
            CheckedEnumCast<EMaintenanceComponent>(request->component()),
            request->address(),
            ids);
    }

    void DoAddMaintenance(
        EMaintenanceComponent component,
        const TString& address,
        TMaintenanceId id,
        const TString& user,
        EMaintenanceType type,
        const TString& comment)
    {
        YT_VERIFY(HasMutationContext());

        auto* target = FindComponentOrAlert(component, address);
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

        if (!IsReplicationToSecondaryMastersNeeded()) {
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
        const TCompactVector<TMaintenanceId, TypicalMaintenanceRequestCount>& ids)
    {
        YT_VERIFY(HasMutationContext());

        auto* target = FindComponentOrAlert(component, address);
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

        if (ids.empty() || !IsReplicationToSecondaryMastersNeeded()) {
            return;
        }

        TReqReplicateMaintenanceRequestRemoval mutationRequest;
        mutationRequest.set_component(ToProto<i32>(component));
        mutationRequest.set_address(address);
        ToProto(mutationRequest.mutable_ids(), ids);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->PostToSecondaryMasters(mutationRequest);
    }

    bool IsReplicationToSecondaryMastersNeeded() const
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        return multicellManager->IsPrimaryMaster() && multicellManager->IsMulticell();
    }

    void ValidatePermission(NObjectServer::TObject* object, EPermission permission)
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidatePermission(object, permission);
    }

    TNontemplateMaintenanceTargetBase* GetComponentOrThrow(EMaintenanceComponent component, const TString& address)
    {
        YT_VERIFY(component != EMaintenanceComponent::Host);

        if (component != EMaintenanceComponent::ClusterNode) {
            THROW_ERROR_EXCEPTION("Maintenance component %Qlv is not supported", component)
                << TErrorAttribute("component", component);
        }

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->FindNodeByAddress(address);
        if (!IsObjectAlive(node)) {
            THROW_ERROR_EXCEPTION("No such node %Qv", address)
                << TErrorAttribute("address", address);
        }

        return node;
    }

    TNontemplateMaintenanceTargetBase* FindComponentOrAlert(
        EMaintenanceComponent component,
        const TString& address)
    {
        if (component != EMaintenanceComponent::ClusterNode) {
            YT_LOG_ALERT("Maintenance component is not supported yet (Component: %v)",
                component);
            return nullptr;
        }

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->FindNodeByAddress(address);
        if (!IsObjectAlive(node)) {
            YT_LOG_ALERT("No such node (Address: %v)", address);
            return nullptr;
        }

        return node;
    }

    void VerifyPrimaryMasterMutation()
    {
        YT_VERIFY(HasMutationContext());

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsPrimaryMaster());
    }

    void VerifySecondaryMasterMutation()
    {
        YT_VERIFY(HasMutationContext());

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsSecondaryMaster());
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
