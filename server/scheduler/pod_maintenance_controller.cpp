#include "pod_maintenance_controller.h"

#include "private.h"

#include <yp/server/master/bootstrap.h>

#include <yp/server/objects/node.h>
#include <yp/server/objects/pod.h>
#include <yp/server/objects/transaction.h>
#include <yp/server/objects/transaction_manager.h>

#include <yp/server/lib/cluster/cluster.h>
#include <yp/server/lib/cluster/node.h>
#include <yp/server/lib/cluster/pod.h>

namespace NYP::NServer::NScheduler {

using namespace NMaster;
using namespace NObjects;

using namespace NYT::NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TPodMaintenanceController::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    void AbortEviction(const NCluster::TClusterPtr& cluster)
    {
        auto check = [] (const auto* pod, const auto* node) {
            if (!GetEnableScheduling(pod)) {
                return false;
            }
            const auto& podEviction = GetPodEviction(pod);
            auto hfsmState = GetHfsmState(node);
            return podEviction.reason() == NClient::NApi::NProto::ER_HFSM &&
                podEviction.state() == NClient::NApi::NProto::ES_REQUESTED &&
                hfsmState == EHfsmState::Up;
        };
        auto transition = [] (NObjects::TPod* pod, NObjects::TNode* node) {
            auto hfsmState = GetHfsmState(node);

            YT_LOG_DEBUG("Pod eviction aborted by maintenance controller (PodId: %v, NodeId: %v, HfsmState: %v)",
                pod->GetId(),
                node->GetId(),
                hfsmState);

            pod->UpdateEvictionStatus(
                EEvictionState::None,
                EEvictionReason::None,
                Format("Eviction aborted due to node %Qv being in %Qlv state",
                    node->GetId(),
                    hfsmState));
        };
        ExecuteTransition(
            cluster,
            check,
            transition,
            "AbortEviction");
    }

    void RequestEviction(const NCluster::TClusterPtr& cluster)
    {
        auto check = [] (const auto* pod, const auto* node) {
            if (!GetEnableScheduling(pod)) {
                return false;
            }
            const auto& podEviction = GetPodEviction(pod);
            auto hfsmState = GetHfsmState(node);
            return podEviction.state() == NClient::NApi::NProto::ES_NONE &&
                (hfsmState == EHfsmState::PrepareMaintenance || hfsmState == EHfsmState::Down);
        };
        auto transition = [] (NObjects::TPod* pod, NObjects::TNode* node) {
            auto hfsmState = GetHfsmState(node);

            YT_LOG_DEBUG("Pod eviction requested by maintenance controller (PodId: %v, NodeId: %v, HfsmState: %v)",
                pod->GetId(),
                node->GetId(),
                hfsmState);

            pod->RequestEviction(
                EEvictionReason::Hfsm,
                Format("Eviction requested due to node %Qv being in %Qlv state",
                    node->GetId(),
                    hfsmState),
                /*validateDisruptionBudget*/ false);
        };
        ExecuteTransition(
            cluster,
            check,
            transition,
            "RequestEviction");
    }

    void ResetMaintenance(const NCluster::TClusterPtr& cluster)
    {
        auto check = [] (const auto* pod, const auto* node) {
            const auto& podMaintenance = GetPodMaintenance(pod);
            const auto& nodeMaintenance = GetNodeMaintenance(node);
            return podMaintenance.state() != NClient::NApi::NProto::PMS_NONE &&
                nodeMaintenance.state() == NClient::NApi::NProto::NMS_NONE;
        };
        auto transition = [] (NObjects::TPod* pod, NObjects::TNode* node) {
            YT_LOG_DEBUG("Pod maintenance reset by maintenance controller (PodId: %v, NodeId: %v)",
                pod->GetId(),
                node->GetId());

            pod->UpdateMaintenanceStatus(
                EPodMaintenanceState::None,
                Format("Maintenance reset due to node %Qv being in none maintenance state", node->GetId()),
                /* infoUpdate */ TGenericClearUpdate());
        };
        ExecuteTransition(
            cluster,
            check,
            transition,
            "ResetMaintenance");
    }

    void RequestMaintenance(const NCluster::TClusterPtr& cluster)
    {
        auto check = [] (const auto* pod, const auto* node) {
            const auto& podMaintenance = GetPodMaintenance(pod);
            const auto& nodeMaintenance = GetNodeMaintenance(node);
            if (nodeMaintenance.state() != NClient::NApi::NProto::NMS_REQUESTED &&
                nodeMaintenance.state() != NClient::NApi::NProto::NMS_ACKNOWLEDGED)
            {
                return false;
            }
            if (podMaintenance.info().uuid() != nodeMaintenance.info().uuid()) {
                return true;
            }
            if (podMaintenance.state() == NClient::NApi::NProto::PMS_REQUESTED ||
                podMaintenance.state() == NClient::NApi::NProto::PMS_ACKNOWLEDGED)
            {
                return false;
            }
            return true;
        };
        auto transition = [] (NObjects::TPod* pod, NObjects::TNode* node) {
            const auto& nodeMaintenance = GetNodeMaintenance(node);

            YT_LOG_DEBUG("Pod maintenance requested by maintenance controller (PodId: %v, NodeId: %v)",
                pod->GetId(),
                node->GetId());

            pod->UpdateMaintenanceStatus(
                EPodMaintenanceState::Requested,
                Format("Maintenance requested due to node %Qv maintenance", node->GetId()),
                nodeMaintenance.info());
        };
        ExecuteTransition(
            cluster,
            check,
            transition,
            "RequestMaintenance");
    }

    void SyncInProgressMaintenance(const NCluster::TClusterPtr& cluster)
    {
        auto check = [] (const auto* pod, const auto* node) {
            const auto& podMaintenance = GetPodMaintenance(pod);
            const auto& nodeMaintenance = GetNodeMaintenance(node);
            if (nodeMaintenance.state() != NClient::NApi::NProto::NMS_IN_PROGRESS) {
                return false;
            }
            if (podMaintenance.state() == NClient::NApi::NProto::PMS_IN_PROGRESS &&
                podMaintenance.info().uuid() == nodeMaintenance.info().uuid())
            {
                return false;
            }
            return true;
        };
        auto transition = [] (NObjects::TPod* pod, NObjects::TNode* node) {
            const auto& podMaintenance = GetPodMaintenance(pod);
            const auto& nodeMaintenance = GetNodeMaintenance(node);

            YT_LOG_DEBUG("In progress pod maintenance synched by maintenance controller (PodId: %v, NodeId: %v)",
                pod->GetId(),
                node->GetId());

            if (podMaintenance.state() != NClient::NApi::NProto::PMS_ACKNOWLEDGED) {
                YT_LOG_WARNING("Unacknowledged pod maintenance detected (PodId: %v, State: %v)",
                    pod->GetId(),
                    CheckedEnumCast<EPodMaintenanceState>(podMaintenance.state()));
            }

            pod->UpdateMaintenanceStatus(
                EPodMaintenanceState::InProgress,
                Format("Maintenance is in progress due to node %Qv maintenance", node->GetId()),
                nodeMaintenance.info());
        };
        ExecuteTransition(
            cluster,
            check,
            transition,
            "SyncInProgressMaintenance");
    }

    void SyncNodeAlerts(const NCluster::TClusterPtr& cluster)
    {
        // Protobuf does not provide simple comparison mechanics =(
        auto areEqualNodeAlerts = [] (const TNodeAlerts& lhs, const TNodeAlerts& rhs) {
            if (lhs.size() != rhs.size()) {
                return false;
            }
            for (int i = 0; i < lhs.size(); ++i) {
                if (lhs[i].uuid() != rhs[i].uuid()) {
                    return false;
                }
            }
            return true;
        };
        auto check = [&] (const auto* pod, const auto* node) {
            const auto& podNodeAlerts = GetNodeAlerts(pod);
            const auto& nodeAlerts = GetNodeAlerts(node);
            return !areEqualNodeAlerts(podNodeAlerts, nodeAlerts);
        };
        auto transition = [] (NObjects::TPod* pod, NObjects::TNode* node) {
            const auto& nodeAlerts = GetNodeAlerts(node);

            YT_LOG_DEBUG("Pod node alerts set by maintenance controller (PodId: %v, NodeId: %v, Alerts: %v)",
                pod->GetId(),
                node->GetId(),
                nodeAlerts);

            pod->Status().Etc()->mutable_node_alerts()->CopyFrom(nodeAlerts);
        };
        ExecuteTransition(
            cluster,
            check,
            transition,
            "SyncNodeAlerts");
    }

private:
    TBootstrap* const Bootstrap_;


    static bool GetEnableScheduling(const NCluster::TPod* pod)
    {
        return pod->GetEnableScheduling();
    }

    static bool GetEnableScheduling(const NObjects::TPod* pod)
    {
        return pod->Spec().EnableScheduling().Load();
    }


    static const NClient::NApi::NProto::TPodStatus_TEviction& GetPodEviction(const NCluster::TPod* pod)
    {
        return pod->Eviction();
    }

    static const NClient::NApi::NProto::TPodStatus_TEviction& GetPodEviction(const NObjects::TPod* pod)
    {
        return pod->Status().Etc().Load().eviction();
    }


    static EHfsmState GetHfsmState(const NCluster::TNode* node)
    {
        return node->GetHfsmState();
    }

    static EHfsmState GetHfsmState(const NObjects::TNode* node)
    {
        return static_cast<EHfsmState>(node->Status().Etc().Load().hfsm().state());
    }


    static const TNodeAlerts& GetNodeAlerts(const NCluster::TNode* node)
    {
        return node->Alerts();
    }

    static const TNodeAlerts& GetNodeAlerts(const NObjects::TNode* node)
    {
        return node->Status().Etc().Load().alerts();
    }

    static const TNodeAlerts& GetNodeAlerts(const NCluster::TPod* pod)
    {
        return pod->NodeAlerts();
    }

    static const TNodeAlerts& GetNodeAlerts(const NObjects::TPod* pod)
    {
        return pod->Status().Etc().Load().node_alerts();
    }


    static const NClient::NApi::NProto::TNodeStatus_TMaintenance& GetNodeMaintenance(const NCluster::TNode* node)
    {
        return node->Maintenance();
    }

    static const NClient::NApi::NProto::TNodeStatus_TMaintenance& GetNodeMaintenance(const NObjects::TNode* node)
    {
        return node->Status().Etc().Load().maintenance();
    }

    static const NClient::NApi::NProto::TPodStatus_TMaintenance& GetPodMaintenance(const NCluster::TPod* pod)
    {
        return pod->Maintenance();
    }

    static const NClient::NApi::NProto::TPodStatus_TMaintenance& GetPodMaintenance(const NObjects::TPod* pod)
    {
        return pod->Status().Etc().Load().maintenance();
    }


    template <class TCheck, class TTransition>
    void ExecuteTransitionForPod(
        const TTransactionPtr& transaction,
        TCheck&& check,
        TTransition&& transition,
        const NCluster::TPod* pod,
        TStringBuf transitionName)
    {
        const auto* node = pod->GetNode();
        if (!node || !check(pod, node)) {
            return;
        }

        YT_LOG_DEBUG("Pod maintenance transition check succeeded (PodId: %v, TransitionName: %v)",
            pod->GetId(),
            transitionName);

        auto* transactionPod = transaction->GetPod(pod->GetId());
        if (!transactionPod->DoesExist()) {
            YT_LOG_DEBUG("Pod maintenance transition is skipped since transaction pod does not exist (PodId: %v, TransitionName: %v)",
                pod->GetId(),
                transitionName);
            return;
        }

        transactionPod->Status().Etc().ScheduleLoad();

        auto* transactionNode = transactionPod->Spec().Node().Load();
        if (!transactionNode) {
            YT_LOG_DEBUG("Pod maintenance transition is skipped since transaction pod is not assigned to any node (PodId: %v, TransitionName: %v)",
                pod->GetId(),
                transitionName);
            return;
        }

        transactionNode->Status().Etc().ScheduleLoad();

        if (!check(transactionPod, transactionNode)) {
            YT_LOG_DEBUG("Pod maintenance transition is skipped since transaction check failed (PodId: %v, TransitionName: %v)",
                pod->GetId(),
                transitionName);
            return;
        }

        transition(transactionPod, transactionNode);
    }

    template <class TCheck, class TTransition>
    void ExecuteTransition(
        const NCluster::TClusterPtr& cluster,
        TCheck&& check,
        TTransition&& transition,
        TStringBuf transitionName)
    {
        try {
            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            auto transaction = WaitFor(transactionManager->StartReadWriteTransaction())
                .ValueOrThrow();

            YT_LOG_DEBUG("Started pod maintenance transition transaction (TransactionId: %v, StartTimestamp: %llx, TransitionName: %v)",
                transaction->GetId(),
                transaction->GetStartTimestamp(),
                transitionName);

            auto pods = cluster->GetPods();
            for (auto* pod : pods) {
                ExecuteTransitionForPod(
                    transaction,
                    check,
                    transition,
                    pod,
                    transitionName);
            }

            WaitFor(transaction->Commit())
                .ThrowOnError();
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error executing pod maintenance transition (TransitionName: %v)",
                transitionName);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TPodMaintenanceController::TPodMaintenanceController(TBootstrap* bootstrap)
    : Impl_(New<TPodMaintenanceController::TImpl>(bootstrap))
{ }

void TPodMaintenanceController::AbortEviction(const NCluster::TClusterPtr& cluster)
{
    Impl_->AbortEviction(cluster);
}

void TPodMaintenanceController::RequestEviction(const NCluster::TClusterPtr& cluster)
{
    Impl_->RequestEviction(cluster);
}

void TPodMaintenanceController::ResetMaintenance(const NCluster::TClusterPtr& cluster)
{
    Impl_->ResetMaintenance(cluster);
}

void TPodMaintenanceController::RequestMaintenance(const NCluster::TClusterPtr& cluster)
{
    Impl_->RequestMaintenance(cluster);
}

void TPodMaintenanceController::SyncInProgressMaintenance(const NCluster::TClusterPtr& cluster)
{
    Impl_->SyncInProgressMaintenance(cluster);
}

void TPodMaintenanceController::SyncNodeAlerts(const NCluster::TClusterPtr& cluster)
{
    Impl_->SyncNodeAlerts(cluster);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
