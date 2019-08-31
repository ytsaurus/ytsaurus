#include "scheduler.h"

#include "allocation_plan.h"
#include "cluster_reader.h"
#include "config.h"
#include "global_resource_allocator.h"
#include "helpers.h"
#include "label_filter_evaluator.h"
#include "pod_disruption_budget_controller.h"
#include "resource_manager.h"
#include "schedule_queue.h"

#include <yp/server/master/yt_connector.h>
#include <yp/server/master/bootstrap.h>

#include <yp/server/objects/node.h>
#include <yp/server/objects/object_manager.h>
#include <yp/server/objects/pod.h>
#include <yp/server/objects/transaction.h>
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

class TPodEvictionByHfsmController
{
public:
    explicit TPodEvictionByHfsmController(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    void AbortPodEvictionAtUpNodes(const NCluster::TClusterPtr& cluster)
    {
        auto check = [] (const auto* pod, const auto* node) {
            const auto& podEviction = GetPodEviction(pod);
            auto hfsmState = GetHfsmState(node);
            return podEviction.reason() == NClient::NApi::NProto::ER_HFSM &&
                podEviction.state() == NClient::NApi::NProto::ES_REQUESTED &&
                hfsmState == EHfsmState::Up;
        };
        auto transition = [] (NObjects::TPod* pod, NObjects::TNode* node) {
            auto hfsmState = GetHfsmState(node);

            YT_LOG_DEBUG("Pod eviction aborted by HFSM (PodId: %v, NodeId: %v, HfsmState: %v)",
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
            "AbortPodEvictionAtUpNodes");
    }

    void RequestPodEvictionAtNodesWithRequestedMaintenance(const NCluster::TClusterPtr& cluster)
    {
        auto check = [] (const auto* pod, const auto* node) {
            const auto& podEviction = GetPodEviction(pod);
            auto hfsmState = GetHfsmState(node);
            return podEviction.state() == NClient::NApi::NProto::ES_NONE &&
                (hfsmState == EHfsmState::PrepareMaintenance || hfsmState == EHfsmState::Down);
        };
        auto transition = [] (NObjects::TPod* pod, NObjects::TNode* node) {
            auto hfsmState = GetHfsmState(node);

            YT_LOG_DEBUG("Pod eviction requested by HFSM (PodId: %v, NodeId: %v, HfsmState: %v)",
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
            "RequestPodEvictionAtNodesWithRequestedMaintenance");
    }

private:
    TBootstrap* const Bootstrap_;

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

    template <class TCheck, class TTransition>
    void ExecuteTransitionForPod(
        const TTransactionPtr& transaction,
        TCheck&& check,
        TTransition&& transition,
        const NCluster::TPod* pod,
        TStringBuf transitionName)
    {
        const auto* node = pod->GetNode();
        if (!node) {
            return;
        }
        if (!check(pod, node)) {
            return;
        }

        YT_LOG_DEBUG("Pod eviction by HFSM check succeeds (PodId: %v, TransitionName: %v)",
            pod->GetId(),
            transitionName);

        auto* transactionPod = transaction->GetPod(pod->GetId());
        if (!transactionPod->DoesExist()) {
            YT_LOG_DEBUG("Pod eviction by HFSM is skipped since transaction pod does not exist (PodId: %v, TransitionName: %v)",
                pod->GetId(),
                transitionName);
            return;
        }

        auto* transactionNode = transactionPod->Spec().Node().Load();
        if (!transactionNode) {
            YT_LOG_DEBUG("Pod eviction by HFSM is skipped since transaction pod is not assigned to any node (PodId: %v, TransitionName: %v)",
                pod->GetId(),
                transitionName);
            return;
        }

        if (!check(transactionPod, transactionNode)) {
            YT_LOG_DEBUG("Pod eviction by HFSM is skipped since transaction check fails (PodId: %v, TransitionName: %v)",
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

            YT_LOG_DEBUG("Started transaction for pod eviction by HFSM (TransactionId: %v, StartTimestamp: %llx, TransitionName: %v)",
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
            YT_LOG_DEBUG(ex, "Error executing pod eviction by HFSM (TransitionName: %v)",
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
        NCluster::TClusterPtr Cluster;
        TScheduleQueuePtr ScheduleQueue;
    };

    TLoopContext GlobalLoopContext_;


    class TLoopIteration
        : public TRefCounted
    {
    public:
        explicit TLoopIteration(TBootstrap* bootstrap, TLoopContext context)
            : Bootstrap_(bootstrap)
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
            PROFILE_TIMING("/loop/time/reconcile_state") {
                ReconcileState();
            }
            if (shouldRunStage(ESchedulerLoopStage::UpdateNodeSegmentsStatus)) {
                const auto& accountingManager = Bootstrap_->GetAccountingManager();
                PROFILE_TIMING("/loop/time/update_node_segments_status") {
                    accountingManager->UpdateNodeSegmentsStatus(Context_.Cluster);
                }
            }
            if (shouldRunStage(ESchedulerLoopStage::UpdateAccountsStatus)) {
                const auto& accountingManager = Bootstrap_->GetAccountingManager();
                PROFILE_TIMING("/loop/time/update_accounts_status") {
                    accountingManager->UpdateAccountsStatus(Context_.Cluster);
                }
            }
            if (shouldRunStage(ESchedulerLoopStage::RunPodDisruptionBudgetController)) {
                PROFILE_TIMING("/loop/time/run_pod_disruption_budget_controller") {
                    Context_.PodDisruptionBudgetController->Run(Context_.Cluster);
                }
            }
            if (shouldRunStage(ESchedulerLoopStage::RunPodEvictionByHfsmController)) {
                TPodEvictionByHfsmController podEvictionByHfsmController(Bootstrap_);
                PROFILE_TIMING("/loop/time/abort_requested_pod_eviction_at_up_nodes") {
                    podEvictionByHfsmController.AbortPodEvictionAtUpNodes(Context_.Cluster);
                }
                PROFILE_TIMING("/loop/time/request_pod_eviction_at_nodes_with_requested_maintenance") {
                    podEvictionByHfsmController.RequestPodEvictionAtNodesWithRequestedMaintenance(Context_.Cluster);
                }
            }
            if (shouldRunStage(ESchedulerLoopStage::RevokePodsWithAcknowledgedEviction)) {
                PROFILE_TIMING("/loop/time/revoke_pods_with_acknowledged_eviction") {
                    RevokePodsWithAcknowledgedEviction();
                }
            }
            if (shouldRunStage(ESchedulerLoopStage::RemoveOrphanedAllocations)) {
                PROFILE_TIMING("/loop/time/remove_orphaned_allocations") {
                    RemoveOrphanedAllocations();
                }
            }
            if (shouldRunStage(ESchedulerLoopStage::AcknowledgeNodeMaintenance)) {
                PROFILE_TIMING("/loop/time/acknowledge_node_maintenance") {
                    AcknowledgeNodeMaintenance();
                }
            }
            if (shouldRunStage(ESchedulerLoopStage::SchedulePods)) {
                PROFILE_TIMING("/loop/time/schedule_pods") {
                    SchedulePods();
                }
            }
            PROFILE_TIMING("/loop/time/commit") {
                Commit();
            }
        }

    private:
        TBootstrap* const Bootstrap_;
        const TLoopContext Context_;

        NNet::TInternetAddressManager InternetAddressManager_;

        TAllocationPlan AllocationPlan_;


        void ReconcileState()
        {
            YT_LOG_DEBUG("Started reconciling state");

            Context_.Cluster->LoadSnapshot();

            auto pods = Context_.Cluster->GetPods();
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

            YT_LOG_DEBUG("State reconciled");
        }

        static void ReconcileInternetAddressManagerState(
            const NCluster::TClusterPtr& cluster,
            NNet::TInternetAddressManager* internetAddressManager)
        {
            NNet::TIP4AddressPoolIdToFreeIP4Addresses ip4AddressPoolIdToFreeAddresses;
            for (auto* address : cluster->GetInternetAddresses()) {
                if (!address->Status().has_pod_id()) {
                    const auto& ip4AddressPoolId = address->ParentId();
                    const auto& networkModuleId = address->Spec().network_module_id();
                    ip4AddressPoolIdToFreeAddresses[std::make_pair(ip4AddressPoolId, networkModuleId)].push(address->GetId());
                }
            }

            internetAddressManager->ReconcileState(std::move(ip4AddressPoolIdToFreeAddresses));
        }

        void RevokePodsWithAcknowledgedEviction()
        {
            auto pods = Context_.Cluster->GetPods();
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

        void AcknowledgeNodeMaintenance()
        {
            try {
                const auto& transactionManager = Bootstrap_->GetTransactionManager();
                auto transaction = WaitFor(transactionManager->StartReadWriteTransaction())
                    .ValueOrThrow();

                auto nodes = Context_.Cluster->GetNodes();
                for (auto* node : nodes) {
                    if (!node->Pods().empty()) {
                        continue;
                    }
                    if (node->GetMaintenanceState() != ENodeMaintenanceState::Requested) {
                        continue;
                    }

                    auto* transactionNode = transaction->GetNode(node->GetId());
                    YT_LOG_DEBUG("Node maintenance acknowledged (NodeId: %v)",
                        node->GetId());
                    transactionNode->UpdateMaintenanceStatus(
                        ENodeMaintenanceState::Acknowledged,
                        "Maintenance acknowledged by scheduler since no pods are left on this node");
                }

                WaitFor(transaction->Commit())
                    .ThrowOnError();
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Error committing node maintenance state transitions");
            }
        }

        void RecordAllocationSuccess(NCluster::TPod* pod, NCluster::TNode* node)
        {
            YT_LOG_DEBUG("Node allocation succeeded (PodId: %v, NodeId: %v)",
                pod->GetId(),
                node->GetId());
            AllocationPlan_.AssignPodToNode(pod, node);
        }

        void RecordAllocationFailure(NCluster::TPod* pod)
        {
            const auto& podId = pod->GetId();
            auto now = TInstant::Now();
            auto deadline = now + Context_.Config->FailedAllocationBackoffTime;
            YT_LOG_DEBUG("Node allocation failed; backing off (PodId: %v, Deadline: %v)",
                podId,
                deadline);
            Context_.ScheduleQueue->Enqueue(podId, deadline);
        }

        void RecordSchedulingFailure(NCluster::TPod* pod, const TError& error)
        {
            const auto& podId = pod->GetId();
            auto now = TInstant::Now();
            auto deadline = now + Context_.Config->FailedAllocationBackoffTime;
            YT_LOG_DEBUG(error, "Pod scheduling failure; backing off (PodId: %v, Deadline: %v)",
                podId,
                deadline);
            Context_.ScheduleQueue->Enqueue(podId, deadline);
            AllocationPlan_.RecordFailure(pod, error);
        }

        void SchedulePods()
        {
            YT_LOG_DEBUG("Started scheduling pods");

            auto now = TInstant::Now();
            while (true) {
                auto podId = Context_.ScheduleQueue->Dequeue(now);
                if (!podId) {
                    break;
                }

                auto* pod = Context_.Cluster->FindPod(podId);
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
                        RecordAllocationSuccess(pod, node);
                    } else {
                        RecordAllocationFailure(pod);
                    }
                } else {
                    RecordSchedulingFailure(pod, nodeOrError);
                }
            }
            YT_LOG_DEBUG("Pods scheduled");
        }

        void Commit()
        {
            {
                YT_LOG_DEBUG("Started committing scheduling results (PodCount: %v, NodeCount: %v, FailureCount: %v)",
                    AllocationPlan_.GetPodCount(),
                    AllocationPlan_.GetNodeCount(),
                    AllocationPlan_.GetFailures().size());

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

            if (!AllocationPlan_.GetFailures().empty()) {
                YT_LOG_DEBUG("Started committing scheduling failures (Count: %v)",
                    AllocationPlan_.GetPodCount(),
                    AllocationPlan_.GetNodeCount(),
                    AllocationPlan_.GetFailures().size());

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
                                nodeRequest && nodeRequest->Type == EAllocationPlanNodeRequestType::RemoveOrphanedResourceScheduledAllocations) {
                            resourceManager->RemoveOrphanedAllocations(transaction, transactionNode);
                            continue;
                        }

                        const auto& podRequest = std::get<TAllocationPlan::TPodRequest>(variantRequest);

                        const auto& podId = podRequest.Pod->GetId();
                        auto* transactionPod = transaction->GetPod(podId);

                        if (!CheckPodSchedulable(transactionPod)) {
                            continue;
                        }

                        if (podRequest.Type == EAllocationPlanPodRequestType::AssignPodToNode) {
                            try {
                                resourceManager->AssignPodToNode(transaction, &resourceManagerContext, transactionNode, transactionPod);
                            } catch (const TErrorException& ex) {
                                if (ex.Error().GetCode() != NClient::NApi::EErrorCode::PodSchedulingFailure) {
                                    throw;
                                }
                                auto error = TError("Error assigning pod to node %Qv",
                                    transactionNode->GetId())
                                    << ex;
                                RecordSchedulingFailure(podRequest.Pod, error);
                            }
                        } else if (podRequest.Type == EAllocationPlanPodRequestType::RevokePodFromNode) {
                            if (transactionPod->Spec().Node().Load() != transactionNode) {
                                YT_LOG_DEBUG("Pod is no longer assigned to the expected node; skipped (PodId: %v, ExpectedNodeId: %v, ActualNodeId: %v)",
                                    podId,
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

                    ToProto(transactionPod->Status().Etc()->mutable_scheduling()->mutable_error(), failure.Error);
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
                    context.Config->PodDisruptionBudgetController);
            } catch (const std::exception& ex) {
                context.PodDisruptionBudgetController = nullptr;
                THROW_ERROR_EXCEPTION("Error creating pod disruption budget controller")
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
        if (!context.Config || !context.GlobalResourceAllocator || !context.PodDisruptionBudgetController) {
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

