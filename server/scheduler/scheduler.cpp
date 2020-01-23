#include "scheduler.h"

#include "allocation_plan.h"
#include "cluster_reader.h"
#include "config.h"
#include "global_resource_allocator.h"
#include "helpers.h"
#include "label_filter_evaluator.h"
#include "pod_disruption_budget_controller.h"
#include "pod_exponential_backoff_policy.h"
#include "resource_manager.h"
#include "schedule_queue.h"

#include <yp/server/master/yt_connector.h>
#include <yp/server/master/bootstrap.h>

#include <yp/server/objects/node.h>
#include <yp/server/objects/object_manager.h>
#include <yp/server/objects/pod.h>
#include <yp/server/objects/transaction_manager.h>

#include <yp/server/accounting/accounting_manager.h>

#include <yp/server/net/internet_address_manager.h>
#include <yp/server/net/net_manager.h>

#include <yp/server/lib/cluster/cluster.h>
#include <yp/server/lib/cluster/internet_address.h>
#include <yp/server/lib/cluster/node.h>
#include <yp/server/lib/cluster/pod.h>
#include <yp/server/lib/cluster/resource.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/periodic_executor.h>

namespace NYP::NServer::NScheduler {

using namespace NServer::NMaster;
using namespace NServer::NObjects;

using namespace NYT::NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TPodMaintenanceController
{
public:
    explicit TPodMaintenanceController(TBootstrap* bootstrap)
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

class TNodeMaintenanceController
{
public:
    explicit TNodeMaintenanceController(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    void Acknowledge(const NCluster::TClusterPtr& cluster)
    {
        ExecuteTransition(
            cluster,
            AcknowledgeTransition,
            "Acknowledge");
    }

private:
    TBootstrap* const Bootstrap_;


    static bool ArePodMaintenancesAcknowledged(const NCluster::TNode* node)
    {
        for (auto* pod : node->Pods()) {
            if (pod->Maintenance().state() != NClient::NApi::NProto::PMS_ACKNOWLEDGED) {
                return false;
            }
        }
        return true;
    }

    static void AcknowledgeTransition(const TTransactionPtr& transaction, const NCluster::TNode* node)
    {
        const auto& maintenance = node->Maintenance();

        if (maintenance.state() != NClient::NApi::NProto::NMS_REQUESTED) {
            return;
        }

        if (!ArePodMaintenancesAcknowledged(node)) {
            return;
        }

        auto* transactionNode = transaction->GetNode(node->GetId());

        transactionNode->Status().Etc().ScheduleLoad();

        if (!transactionNode->DoesExist()) {
            YT_LOG_DEBUG("Node maintenance acknowledgement is skipped since transaction node does not exist (NodeId: %v)",
                node->GetId());
            return;
        }
        const auto& transactionMaintenance = transactionNode->Status().Etc().Load().maintenance();
        if (transactionMaintenance.state() != NClient::NApi::NProto::NMS_REQUESTED) {
            YT_LOG_DEBUG("Node maintenance acknowledgement is skipped since maintenance is not requested for transaction node (NodeId: %v)",
                node->GetId());
            return;
        }
        if (transactionMaintenance.info().uuid() != maintenance.info().uuid()) {
            YT_LOG_DEBUG("Node maintenance acknowledgement is skipped since maintenance request has different uuid in transaction (NodeId: %v, Uuid: %v, TransactionUuid: %v)",
                node->GetId(),
                maintenance.info().uuid(),
                transactionMaintenance.info().uuid());
            return;
        }

        YT_LOG_DEBUG("Node maintenance acknowledged (NodeId: %v)",
            node->GetId());

        transactionNode->UpdateMaintenanceStatus(
            ENodeMaintenanceState::Acknowledged,
            "Maintenance acknowledged by scheduler since all pod maintenancens are acknowledged on this node",
            TGenericPreserveUpdate());
    }

    template <class TTransition>
    void ExecuteTransition(
        const NCluster::TClusterPtr& cluster,
        TTransition&& transition,
        TStringBuf transitionName)
    {
        try {
            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            auto transaction = WaitFor(transactionManager->StartReadWriteTransaction())
                .ValueOrThrow();

            YT_LOG_DEBUG("Started node maintenance transition transaction (TransactionId: %v, StartTimestamp: %llx, TransitionName: %v)",
                transaction->GetId(),
                transaction->GetStartTimestamp(),
                transitionName);

            auto nodes = cluster->GetNodes();
            for (auto* node : nodes) {
                transition(transaction, node);
            }

            WaitFor(transaction->Commit())
                .ThrowOnError();
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error executing node maintenance transition (TransitionName: %v)",
                transitionName);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TScheduler::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TBootstrap* bootstrap,
        TSchedulerConfigPtr config)
        : Bootstrap_(bootstrap)
        , InitialConfig_(std::move(config))
        , SchedulingLoopExecutor_(New<TPeriodicExecutor>(
            LoopActionQueue_->GetInvoker(),
            BIND(&TImpl::OnSchedulingLoop, MakeWeak(this)),
            InitialConfig_->LoopPeriod))
        , GlobalLoopContext_(Bootstrap_)
    {
        VERIFY_INVOKER_THREAD_AFFINITY(LoopActionQueue_->GetInvoker(), LoopThread);
    }

    void Initialize()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // Initialize the loop.
        WaitFor(UpdateLoopConfig(InitialConfig_))
            .ThrowOnError();

        const auto& ytConnector = Bootstrap_->GetYTConnector();
        ytConnector->SubscribeStartedLeading(BIND(&TImpl::OnStartedLeading, MakeWeak(this)));
        ytConnector->SubscribeStoppedLeading(BIND(&TImpl::OnStoppedLeading, MakeWeak(this)));
    }

    TFuture<void> UpdateConfig(TSchedulerConfigPtr config)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Updating scheduler configuration");
        SchedulingLoopExecutor_->SetPeriod(config->LoopPeriod);
        return UpdateLoopConfig(std::move(config));
    }

private:
    TBootstrap* const Bootstrap_;
    const TSchedulerConfigPtr InitialConfig_;

    const TActionQueuePtr LoopActionQueue_ = New<TActionQueue>("SchedulerLoop");
    const TPeriodicExecutorPtr SchedulingLoopExecutor_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(LoopThread);

    struct TAllocationPlanProfiling
    {
        TAllocationPlanProfiling()
            : AssignPodToNodeCounter("/allocation_plan/assign_pod_to_node")
            , RevokePodFromNodeCounter("/allocation_plan/revoke_pod_from_node")
            , RemoveOrphanedAllocationsCounter("/allocation_plan/remove_orphaned_allocations")
            , ComputeAllocationFailureCounter("/allocation_plan/compute_allocation_failure")
            , AssignPodToNodeFailureCounter("/allocation_plan/assign_pod_to_node_failure")
        { }

        NProfiling::TSimpleGauge AssignPodToNodeCounter;
        NProfiling::TSimpleGauge RevokePodFromNodeCounter;
        NProfiling::TSimpleGauge RemoveOrphanedAllocationsCounter;
        NProfiling::TSimpleGauge ComputeAllocationFailureCounter;
        NProfiling::TSimpleGauge AssignPodToNodeFailureCounter;
    };

    struct TLoopContext
    {
        explicit TLoopContext(TBootstrap* bootstrap)
            : Cluster(New<NCluster::TCluster>(
                Logger,
                Profiler.AppendPath("/cluster_snapshot"),
                CreateClusterReader(bootstrap),
                CreateLabelFilterEvaluator(bootstrap)))
            , ScheduleQueue(New<TScheduleQueue>())
        { }

        TSchedulerConfigPtr Config;
        IGlobalResourceAllocatorPtr GlobalResourceAllocator;
        TPodDisruptionBudgetControllerPtr PodDisruptionBudgetController;
        TPodExponentialBackoffPolicyPtr FailedAllocationBackoffPolicy;
        NCluster::TClusterPtr Cluster;
        TScheduleQueuePtr ScheduleQueue;
        TAllocationPlanProfiling AllocationPlanProfiling;
    };

    TLoopContext GlobalLoopContext_;

    class TLoopIteration
        : public TRefCounted
    {
    public:
        explicit TLoopIteration(TBootstrap* bootstrap, TLoopContext context)
            : Bootstrap_(bootstrap)
            , Profiler(NScheduler::Profiler.AppendPath("/loop"))
            , Context_(std::move(context))
        { }

        void Run()
        {
            auto shouldRunStage = [this] (ESchedulerLoopStage stage) {
                if (Context_.Config->DisableStage[stage]) {
                    YT_LOG_INFO("Skipping disabled stage %v",
                        stage);
                    return false;
                }
                return true;
            };
            PROFILE_TIMING("/time/reconcile_state") {
                ReconcileState();
            }
            if (shouldRunStage(ESchedulerLoopStage::UpdateNodeSegmentsStatus)) {
                const auto& accountingManager = Bootstrap_->GetAccountingManager();
                PROFILE_TIMING("/time/update_node_segments_status") {
                    accountingManager->UpdateNodeSegmentsStatus(Context_.Cluster);
                }
            }
            if (shouldRunStage(ESchedulerLoopStage::UpdateAccountsStatus)) {
                const auto& accountingManager = Bootstrap_->GetAccountingManager();
                PROFILE_TIMING("/time/update_accounts_status") {
                    accountingManager->UpdateAccountsStatus(Context_.Cluster);
                }
            }
            if (shouldRunStage(ESchedulerLoopStage::RunPodDisruptionBudgetController)) {
                PROFILE_TIMING("/time/run_pod_disruption_budget_controller") {
                    Context_.PodDisruptionBudgetController->Run(Context_.Cluster);
                }
            }
            if (shouldRunStage(ESchedulerLoopStage::RunPodMaintenanceController)) {
                TPodMaintenanceController podMaintenanceController(Bootstrap_);

                // COMPAT(bidzilya): Do not request eviction explicitly, let controllers decide.
                PROFILE_TIMING("/time/pod_maintenance/abort_eviction") {
                    podMaintenanceController.AbortEviction(Context_.Cluster);
                }
                PROFILE_TIMING("/time/pod_maintenance/request_eviction") {
                    podMaintenanceController.RequestEviction(Context_.Cluster);
                }

                PROFILE_TIMING("/time/pod_maintenance/reset_maintenance") {
                    podMaintenanceController.ResetMaintenance(Context_.Cluster);
                }
                PROFILE_TIMING("/time/pod_maintenance/request_maintenance") {
                    podMaintenanceController.RequestMaintenance(Context_.Cluster);
                }
                PROFILE_TIMING("/time/pod_maintenance/sync_in_progress_maintenance") {
                    podMaintenanceController.SyncInProgressMaintenance(Context_.Cluster);
                }

                PROFILE_TIMING("/time/pod_maintenance/sync_node_alerts") {
                    podMaintenanceController.SyncNodeAlerts(Context_.Cluster);
                }
            }
            if (shouldRunStage(ESchedulerLoopStage::RevokePodsWithAcknowledgedEviction)) {
                PROFILE_TIMING("/time/revoke_pods_with_acknowledged_eviction") {
                    RevokePodsWithAcknowledgedEviction();
                }
            }
            if (shouldRunStage(ESchedulerLoopStage::RemoveOrphanedAllocations)) {
                PROFILE_TIMING("/time/remove_orphaned_allocations") {
                    RemoveOrphanedAllocations();
                }
            }
            {
                TNodeMaintenanceController nodeMaintenanceController(Bootstrap_);
                if (shouldRunStage(ESchedulerLoopStage::AcknowledgeNodeMaintenance)) {
                    PROFILE_TIMING("/time/node_maintenance/acknowledge") {
                        nodeMaintenanceController.Acknowledge(Context_.Cluster);
                    }
                }
            }
            if (shouldRunStage(ESchedulerLoopStage::SchedulePods)) {
                PROFILE_TIMING("/time/schedule_pods") {
                    SchedulePods(Context_.Config->SchedulePodsStage);
                }
            }
            PROFILE_TIMING("/time/commit") {
                Commit();
            }
        }

    private:
        TBootstrap* const Bootstrap_;
        const NProfiling::TProfiler Profiler;

        TLoopContext Context_;

        NNet::TInternetAddressManager InternetAddressManager_;

        TAllocationPlan AllocationPlan_;


        void ReconcileState()
        {
            YT_LOG_DEBUG("Started reconciling state");

            Context_.Cluster->LoadSnapshot(Context_.Config->Cluster);

            auto pods = Context_.Cluster->GetSchedulablePods();
            auto now = TInstant::Now();
            for (auto* pod : pods) {
                if (!pod->GetNode()) {
                    YT_LOG_DEBUG("Adding pod to schedule queue since it is not assigned to any node (PodId: %v)",
                        pod->GetId());
                    Context_.ScheduleQueue->Enqueue(pod->GetId(), now);
                }
            }

            // TODO(babenko): profiling
            AllocationPlan_.Clear();
            ReconcileInternetAddressManagerState(Context_.Cluster, &InternetAddressManager_);
            Context_.GlobalResourceAllocator->ReconcileState(Context_.Cluster);
            Context_.FailedAllocationBackoffPolicy->ReconcileState(Context_.Cluster);

            YT_LOG_DEBUG("State reconciled");
        }

        static void ReconcileInternetAddressManagerState(
            const NCluster::TClusterPtr& cluster,
            NNet::TInternetAddressManager* internetAddressManager)
        {
            NNet::TIP4AddressesPerPoolAndNetworkModule freeAddresses;
            for (auto* address : cluster->GetInternetAddresses()) {
                if (!address->Status().has_pod_id()) {
                    const auto& ip4AddressPoolId = address->ParentId();
                    const auto& networkModuleId = address->Spec().network_module_id();
                    freeAddresses[std::make_pair(ip4AddressPoolId, networkModuleId)].push(address->GetId());
                }
            }

            internetAddressManager->ReconcileState(std::move(freeAddresses));
        }

        void RevokePodsWithAcknowledgedEviction()
        {
            auto pods = Context_.Cluster->GetSchedulablePods();
            for (auto* pod : pods) {
                if (pod->Eviction().state() == NClient::NApi::NProto::ES_ACKNOWLEDGED) {
                    YT_LOG_DEBUG("Pod eviction acknowledged (PodId: %v, NodeId: %v)",
                        pod->GetId(),
                        pod->GetNode()->GetId());
                    AllocationPlan_.RevokePodFromNode(pod);
                }
            }
        }

        void RemoveOrphanedAllocations()
        {
            std::vector<NCluster::TNode*> changedNodes;
            for (auto* resource : Context_.Cluster->GetResources()) {
                auto* node = resource->GetNode();
                for (const auto& allocation : resource->ScheduledAllocations()) {
                    auto* pod = Context_.Cluster->FindPod(allocation.pod_id());
                    if (!pod || pod->Uuid() != allocation.pod_uuid()) {
                        changedNodes.push_back(node);
                        break;
                    } else if (pod) {
                        auto* podNode = pod->GetNode();
                        if (!podNode || podNode->GetId() != node->GetId()) {
                            changedNodes.push_back(node);
                            break;
                        }
                    }
                }
            }
            std::sort(changedNodes.begin(), changedNodes.end());
            changedNodes.erase(std::unique(changedNodes.begin(), changedNodes.end()), changedNodes.end());
            for (auto* node : changedNodes) {
                YT_LOG_DEBUG("Removing orphaned allocations from node (NodeId: %v)",
                    node->GetId());
                AllocationPlan_.RemoveOrphanedAllocations(node);
            }
        }

        void BackoffScheduling(const NCluster::TPod* pod)
        {
            const auto& podId = pod->GetId();

            auto backoffDuration = Context_.FailedAllocationBackoffPolicy->GetNextBackoffDuration(pod);
            auto deadline = TInstant::Now() + backoffDuration;
            YT_LOG_DEBUG("Backing off pod scheduling (PodId: %v, BackoffDuration: %v, Deadline: %v)",
                podId,
                backoffDuration,
                deadline);
            Context_.ScheduleQueue->Enqueue(podId, deadline);
        }

        void SchedulePods(const TSchedulePodsStageConfigPtr& config)
        {
            YT_LOG_DEBUG("Started scheduling pods");

            int podCountProcessed = 0;
            auto startInstant = TInstant::Now();

            while (true) {
                if (podCountProcessed >= config->PodLimit) {
                    YT_LOG_DEBUG("Count of pods for schedule stage exceeded limit; stage finished (PodLimit: %v)",
                        config->PodLimit);
                    break;
                }

                if (TInstant::Now() - startInstant > config->TimeLimit) {
                    YT_LOG_DEBUG("Schedule pods stage exceeded time limit; stage finished (TimeLimit: %v)",
                        config->TimeLimit);
                    break;
                }

                auto podId = Context_.ScheduleQueue->Dequeue(startInstant);
                if (!podId) {
                    break;
                }

                auto* pod = Context_.Cluster->FindSchedulablePod(podId);
                if (!pod) {
                    YT_LOG_DEBUG("Pod no longer exists; discarded (PodId: %v)",
                        podId);
                    continue;
                }

                if (pod->GetNode()) {
                    YT_LOG_DEBUG("Pod is already assigned to node; discarded (PodId: %v, NodeId: %v)",
                        podId,
                        pod->GetNode()->GetId());
                    continue;
                }

                auto nodeOrError = Context_.GlobalResourceAllocator->ComputeAllocation(pod);
                if (nodeOrError.IsOK()) {
                    auto* node = nodeOrError.Value();
                    if (node) {
                        YT_LOG_DEBUG("Node allocation succeeded (PodId: %v, NodeId: %v)",
                            podId,
                            node->GetId());
                        AllocationPlan_.AssignPodToNode(pod, node);
                    } else {
                        YT_LOG_DEBUG("Node allocation failed (PodId: %v)",
                            podId);
                        BackoffScheduling(pod);
                    }
                } else {
                    YT_LOG_DEBUG(nodeOrError, "Pod scheduling failure (PodId: %v)",
                        podId);
                    BackoffScheduling(pod);
                    AllocationPlan_.RecordComputeAllocationFailure(pod, nodeOrError);
                }

                ++podCountProcessed;
            }
            YT_LOG_DEBUG("Pods scheduled");
        }

        void Commit()
        {
            {
                YT_LOG_DEBUG("Started committing scheduling results ("
                    "PodCount: %v, "
                    "NodeCount: %v, "
                    "AssignPodToNodeCount: %v, "
                    "RevokePodFromNodeCount: %v, "
                    "RemoveOrphanedAllocationsCount: %v)",
                    AllocationPlan_.GetPodCount(),
                    AllocationPlan_.GetNodeCount(),
                    AllocationPlan_.GetAssignPodToNodeCount(),
                    AllocationPlan_.GetRevokePodFromNodeCount(),
                    AllocationPlan_.GetRemoveOrphanedAllocationsCount());

                Profiler.Update(
                    Context_.AllocationPlanProfiling.AssignPodToNodeCounter,
                    AllocationPlan_.GetAssignPodToNodeCount());
                Profiler.Update(
                    Context_.AllocationPlanProfiling.RevokePodFromNodeCounter,
                    AllocationPlan_.GetRevokePodFromNodeCount());
                Profiler.Update(
                    Context_.AllocationPlanProfiling.RemoveOrphanedAllocationsCounter,
                    AllocationPlan_.GetRemoveOrphanedAllocationsCount());

                std::vector<TFuture<void>> asyncResults;
                for (int index = 0; index < Context_.Config->AllocationCommitConcurrency; ++index) {
                    asyncResults.push_back(BIND(&TLoopIteration::CommitSchedulingResults, MakeStrong(this))
                        .AsyncVia(GetCurrentInvoker())
                        .Run());
                }
                WaitFor(Combine(asyncResults))
                    .ThrowOnError();

                YT_LOG_DEBUG("Scheduled pods committed");
            }

            Profiler.Update(
                Context_.AllocationPlanProfiling.ComputeAllocationFailureCounter,
                AllocationPlan_.GetComputeAllocationFailureCount());
            Profiler.Update(
                Context_.AllocationPlanProfiling.AssignPodToNodeFailureCounter,
                AllocationPlan_.GetAssignPodToNodeFailureCount());

            if (AllocationPlan_.GetFailureCount() > 0) {
                YT_LOG_DEBUG("Started committing scheduling failures ("
                    "ComputeAllocationFailureCount: %v, "
                    "AssignPodToNodeFailureCount: %v)",
                    AllocationPlan_.GetComputeAllocationFailureCount(),
                    AllocationPlan_.GetAssignPodToNodeFailureCount());

                WaitFor(BIND(&TLoopIteration::CommitSchedulingFailures, MakeStrong(this))
                    .AsyncVia(GetCurrentInvoker())
                    .Run())
                    .ThrowOnError();

                YT_LOG_DEBUG("Scheduling failures committed");
            }
        }

        bool CheckPodSchedulable(NObjects::TPod* pod)
        {
            const auto& podId = pod->GetId();

            if (!pod->DoesExist()) {
                YT_LOG_DEBUG("Pod no longer exists; discarded (PodId: %v)",
                    podId);
                return false;
            }

            if (!pod->Spec().EnableScheduling().Load()) {
                YT_LOG_DEBUG("Pod scheduling disabled; discarded (PodId: %v)",
                    podId);
                return false;
            }

            return true;
        }

        void CommitSchedulingResults()
        {
            while (true) {
                auto optionalPerNodePlan = AllocationPlan_.TryExtractPerNodePlan();
                if (!optionalPerNodePlan) {
                    break;
                }

                const auto& perNodePlan = *optionalPerNodePlan;

                YT_LOG_DEBUG("Committing scheduling results (NodeId: %v, Requests: %v)",
                    perNodePlan.Node->GetId(),
                    MakeFormattableView(perNodePlan.Requests, [] (auto* builder, const auto& request) {
                        FormatValue(builder, request, {});
                    }));

                try {
                    const auto& transactionManager = Bootstrap_->GetTransactionManager();
                    auto transaction = WaitFor(transactionManager->StartReadWriteTransaction())
                        .ValueOrThrow();

                    auto* transactionNode = transaction->GetNode(perNodePlan.Node->GetId());
                    const auto& resourceManager = Bootstrap_->GetResourceManager();
                    const auto& netManager = Bootstrap_->GetNetManager();

                    TResourceManagerContext resourceManagerContext{
                        netManager.Get(),
                        &InternetAddressManager_,
                    };

                    for (const auto& variantRequest : perNodePlan.Requests) {
                        if (auto nodeRequest = std::get_if<TAllocationPlan::TNodeRequest>(&variantRequest);
                            nodeRequest && nodeRequest->Type == EAllocationPlanNodeRequestType::RemoveOrphanedAllocations)
                        {
                            resourceManager->RemoveOrphanedAllocations(transaction, transactionNode);
                            continue;
                        }

                        const auto& podRequest = std::get<TAllocationPlan::TPodRequest>(variantRequest);
                        auto* pod = podRequest.Pod;

                        auto* transactionPod = transaction->GetPod(pod->GetId());

                        if (!CheckPodSchedulable(transactionPod)) {
                            continue;
                        }

                        if (podRequest.Type == EAllocationPlanPodRequestType::AssignPodToNode) {
                            try {
                                resourceManager->AssignPodToNode(transaction, &resourceManagerContext, transactionNode, transactionPod);
                            } catch (const TErrorException& ex) {
                                if (ex.Error().GetCode() == NClient::NApi::EErrorCode::PodSchedulingFailure) {
                                    auto error = TError("Error assigning pod to node %Qv",
                                        transactionNode->GetId())
                                        << ex;
                                    YT_LOG_DEBUG(error, "Pod scheduling failure (PodId: %v)",
                                        pod->GetId());
                                    BackoffScheduling(pod);
                                    AllocationPlan_.RecordAssignPodToNodeFailure(pod, error);
                                }
                                throw;
                            }
                        } else if (podRequest.Type == EAllocationPlanPodRequestType::RevokePodFromNode) {
                            if (transactionPod->Spec().Node().Load() != transactionNode) {
                                YT_LOG_DEBUG("Pod is no longer assigned to the expected node; skipped (PodId: %v, ExpectedNodeId: %v, ActualNodeId: %v)",
                                    pod->GetId(),
                                    perNodePlan.Node->GetId(),
                                    transactionPod->Spec().Node().Load()->GetId());
                                continue;
                            }
                            resourceManager->RevokePodFromNode(transaction, &resourceManagerContext, transactionPod);
                        } else {
                            YT_ABORT();
                        }
                    }

                    WaitFor(transaction->Commit())
                        .ThrowOnError();
                } catch (const std::exception& ex) {
                    auto now = TInstant::Now();
                    YT_LOG_DEBUG(ex, "Error committing pods assignment; will reschedule");
                    for (const auto& variantRequest : perNodePlan.Requests) {
                        if (auto request = std::get_if<TAllocationPlan::TPodRequest>(&variantRequest); request && request->Type == EAllocationPlanPodRequestType::AssignPodToNode) {
                            Context_.ScheduleQueue->Enqueue(request->Pod->GetId(), now);
                        }
                    }
                }
            }
        }

        void CommitSchedulingFailures()
        {
            const auto& failures = AllocationPlan_.GetFailures();

            try {
                const auto& transactionManager = Bootstrap_->GetTransactionManager();
                auto transaction = WaitFor(transactionManager->StartReadWriteTransaction())
                    .ValueOrThrow();

                for (const auto& failure : failures) {
                    const auto& podId = failure.Pod->GetId();
                    auto* transactionPod = transaction->GetPod(podId);
                    if (!CheckPodSchedulable(transactionPod)) {
                        continue;
                    }

                    ToProto(transactionPod->Status().Scheduling().Etc()->mutable_error(), failure.Error);
                }
                WaitFor(transaction->Commit())
                    .ThrowOnError();
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Error committing scheduling failures");
            }
        }
    };

    void UpdateLoopConfigImpl(TSchedulerConfigPtr config)
    {
        VERIFY_THREAD_AFFINITY(LoopThread);

        YT_LOG_INFO("Updating scheduler loop configuration");

        auto& context = GlobalLoopContext_;
        context.Config = std::move(config);
        try {
            try {
                context.GlobalResourceAllocator = CreateGlobalResourceAllocator(
                    context.Config->GlobalResourceAllocator,
                    CreateLabelFilterEvaluator(Bootstrap_));
            } catch (const std::exception& ex) {
                context.GlobalResourceAllocator = nullptr;
                THROW_ERROR_EXCEPTION("Error creating global resource allocator")
                    << ex;
            }
            try {
                context.PodDisruptionBudgetController = New<TPodDisruptionBudgetController>(
                    Bootstrap_,
                    context.Config->PodDisruptionBudgetController,
                    Profiler.AppendPath("/pod_disruption_budget_controller"));
            } catch (const std::exception& ex) {
                context.PodDisruptionBudgetController = nullptr;
                THROW_ERROR_EXCEPTION("Error creating pod disruption budget controller")
                    << ex;
            }
            try {
                context.FailedAllocationBackoffPolicy = New<TPodExponentialBackoffPolicy>(
                    context.Config->FailedAllocationBackoff);
            } catch (const std::exception& ex) {
                context.FailedAllocationBackoffPolicy = nullptr;
                THROW_ERROR_EXCEPTION("Error creating failed allocation backoff policy")
                    << ex;
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error updating scheduler loop configuration")
                << ex;
        }
    }

    TFuture<void> UpdateLoopConfig(TSchedulerConfigPtr config)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return BIND(&TImpl::UpdateLoopConfigImpl, MakeWeak(this), std::move(config))
            .AsyncVia(LoopActionQueue_->GetInvoker())
            .Run();
    }

    static void ValidateLoopContext(const TLoopContext& context)
    {
        if (!context.Config || !context.GlobalResourceAllocator || !context.PodDisruptionBudgetController || !context.FailedAllocationBackoffPolicy) {
            THROW_ERROR_EXCEPTION("Loop is not configured properly");
        }
    }

    void OnSchedulingLoop()
    {
        VERIFY_THREAD_AFFINITY(LoopThread);

        try {
            // NB! Make a copy.
            auto context = GlobalLoopContext_;
            ValidateLoopContext(context);
            if (context.Config->Disabled) {
                YT_LOG_INFO("Scheduler is disabled; skipping loop iteration");
                return;
            }
            New<TLoopIteration>(Bootstrap_, std::move(context))->Run();
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Loop iteration failed");
        }
    }

    void OnStartedLeading()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        SchedulingLoopExecutor_->Start();
    }

    void OnStoppedLeading()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        SchedulingLoopExecutor_->Stop();
    }
};

////////////////////////////////////////////////////////////////////////////////

TScheduler::TScheduler(
    TBootstrap* bootstrap,
    TSchedulerConfigPtr config)
    : Impl_(New<TImpl>(bootstrap, std::move(config)))
{ }

void TScheduler::Initialize()
{
    Impl_->Initialize();
}

TFuture<void> TScheduler::UpdateConfig(TSchedulerConfigPtr config)
{
    return Impl_->UpdateConfig(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler

