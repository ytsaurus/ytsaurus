#include "scheduler.h"
#include "config.h"
#include "cluster.h"
#include "pod.h"
#include "node.h"
#include "schedule_queue.h"
#include "allocation_plan.h"
#include "helpers.h"
#include "resource_manager.h"
#include "global_resource_allocator.h"

#include <yp/server/master/yt_connector.h>
#include <yp/server/master/bootstrap.h>

#include <yp/server/objects/transaction_manager.h>
#include <yp/server/objects/transaction.h>
#include <yp/server/objects/pod.h>
#include <yp/server/objects/node.h>

#include <yp/server/accounting/accounting_manager.h>

#include <yp/server/net/net_manager.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/periodic_executor.h>

namespace NYP::NServer::NScheduler {

using namespace NServer::NMaster;
using namespace NServer::NObjects;

using namespace NYT::NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TScheduler::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TBootstrap* bootstrap,
        TSchedulerConfigPtr config)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
        , SchedulingLoopExecutor_(New<TPeriodicExecutor>(
            LoopActionQueue_->GetInvoker(),
            BIND(&TImpl::OnSchedulingLoop, MakeWeak(this)),
            Config_->LoopPeriod))
        , Cluster_(New<TCluster>(bootstrap))
        , GlobalResourceAllocator_(CreateGlobalResourceAllocator(Config_->GlobalResourceAllocator))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(LoopActionQueue_->GetInvoker(), LoopThread);
    }

    void Initialize()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        ytConnector->SubscribeStartedLeading(BIND(&TImpl::OnStartedLeading, MakeWeak(this)));
        ytConnector->SubscribeStoppedLeading(BIND(&TImpl::OnStoppedLeading, MakeWeak(this)));
    }

    const TClusterPtr& GetCluster() const
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Cluster_;
    }

private:
    TBootstrap* const Bootstrap_;
    const TSchedulerConfigPtr Config_;

    const TActionQueuePtr LoopActionQueue_ = New<TActionQueue>("SchedulerLoop");
    const TPeriodicExecutorPtr SchedulingLoopExecutor_;

    TClusterPtr Cluster_;
    TScheduleQueue ScheduleQueue_;

    IGlobalResourceAllocatorPtr GlobalResourceAllocator_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(LoopThread);


    class TLoopIteration
        : public TRefCounted
    {
    public:
        explicit TLoopIteration(TImplPtr owner)
            : Owner_(std::move(owner))
        { }

        void Run()
        {
            PROFILE_TIMING("/loop/time/reconcile_state") {
                ReconcileState();
            }
            const auto& accountingManager = Owner_->Bootstrap_->GetAccountingManager();
            PROFILE_TIMING("/loop/time/update_node_segments_status") {
                accountingManager->UpdateNodeSegmentsStatus(Owner_->Cluster_);
            }
            PROFILE_TIMING("/loop/time/update_accounts_status") {
                accountingManager->UpdateAccountsStatus(Owner_->Cluster_);
            }
            PROFILE_TIMING("/loop/time/request_pod_eviction_at_nodes_with_requested_maintenance") {
                RequestPodEvictionAtNodesWithRequestedMaintenance();
            }
            PROFILE_TIMING("/loop/time/revoke_pods_with_acknowledged_eviction") {
                RevokePodsWithAcknowledgedEviction();
            }
            PROFILE_TIMING("/loop/time/remove_orphaned_allocations") {
                RemoveOrphanedAllocations();
            }
            PROFILE_TIMING("/loop/time/acknowledge_node_maintenance") {
                AcknowledgeNodeMaintenance();
            }
            PROFILE_TIMING("/loop/time/schedule_pods") {
                SchedulePods();
            }
            PROFILE_TIMING("/loop/time/commit") {
                Commit();
            }
        }

    private:
        const TImplPtr Owner_;

        TInternetAddressManager InternetAddressManager_;

        TAllocationPlan AllocationPlan_;


        void ReconcileState()
        {
            YT_LOG_DEBUG("Started reconciling state");

            Owner_->Cluster_->LoadSnapshot();

            auto pods = Owner_->Cluster_->GetPods();
            auto now = TInstant::Now();
            for (auto* pod : pods) {
                if (!pod->GetNode()) {
                    YT_LOG_DEBUG("Adding pod to schedule queue since it is not assigned to any node (PodId: %v)",
                        pod->GetId());
                    Owner_->ScheduleQueue_.Enqueue(pod->GetId(), now);
                }
            }

            // TODO(babenko): profiling
            AllocationPlan_.Clear();
            InternetAddressManager_.ReconcileState(Owner_->Cluster_);
            Owner_->GlobalResourceAllocator_->ReconcileState(Owner_->Cluster_);

            YT_LOG_DEBUG("State reconciled");
        }

        void RequestPodEvictionAtNodesWithRequestedMaintenance()
        {
            try {
                const auto& transactionManager = Owner_->Bootstrap_->GetTransactionManager();
                auto transaction = WaitFor(transactionManager->StartReadWriteTransaction())
                    .ValueOrThrow();

                auto pods = Owner_->Cluster_->GetPods();
                for (auto* pod : pods) {
                    if (pod->StatusOther().eviction().state() == NClient::NApi::NProto::ES_REQUESTED) {
                        continue;
                    }

                    auto* node = pod->GetNode();
                    if (!node) {
                        continue;
                    }

                    if (node->GetHfsmState() != EHfsmState::PrepareMaintenance &&
                        node->GetHfsmState() != EHfsmState::Down)
                    {
                        continue;
                    }

                    auto* transactionPod = transaction->GetPod(pod->GetId());
                    YT_LOG_DEBUG("Pod eviction requested by HFSM (PodId: %v, NodeId: %v, HfsmState: %v)",
                        pod->GetId(),
                        node->GetId(),
                        node->GetHfsmState());
                    transactionPod->UpdateEvictionStatus(
                        EEvictionState::Requested,
                        EEvictionReason::Hfsm,
                        Format("Eviction requested due to node %Qv being in %Qlv state",
                            node->GetId(),
                            node->GetHfsmState()));
                }
                WaitFor(transaction->Commit())
                    .ThrowOnError();
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Error committing pod eviction state transitions");
            }
        }

        void RevokePodsWithAcknowledgedEviction()
        {
            auto pods = Owner_->Cluster_->GetPods();
            for (auto* pod : pods) {
                if (pod->StatusOther().eviction().state() == NClient::NApi::NProto::ES_ACKNOWLEDGED) {
                    YT_LOG_DEBUG("Pod eviction acknowledged (PodId: %v, NodeId: %v)",
                        pod->GetId(),
                        pod->GetNode()->GetId());
                    AllocationPlan_.RevokePodFromNode(pod);
                }
            }
        }

        void RemoveOrphanedAllocations()
        {
            auto nodes = Owner_->Cluster_->GetNodes();
            for (auto* node : nodes) {
                YT_LOG_DEBUG("Removing orphaned allocations from node (NodeId: %v)",
                    node->GetId());
                AllocationPlan_.RemoveOrphanedAllocations(node);
            }
        }

        void AcknowledgeNodeMaintenance()
        {
            try {
                const auto& transactionManager = Owner_->Bootstrap_->GetTransactionManager();
                auto transaction = WaitFor(transactionManager->StartReadWriteTransaction())
                    .ValueOrThrow();

                auto nodes = Owner_->Cluster_->GetNodes();
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

        void RecordAllocationSuccess(TPod* pod, TNode* node)
        {
            YT_LOG_DEBUG("Node allocation succeeded (PodId: %v, NodeId: %v)",
                pod->GetId(),
                node->GetId());
            AllocationPlan_.AssignPodToNode(pod, node);
        }

        void RecordAllocationFailure(TPod* pod)
        {
            const auto& podId = pod->GetId();
            auto now = TInstant::Now();
            auto deadline = now + Owner_->Config_->FailedAllocationBackoffTime;
            YT_LOG_DEBUG("Node allocation failed; backing off (PodId: %v, Deadline: %v)",
                podId,
                deadline);
            Owner_->ScheduleQueue_.Enqueue(podId, deadline);
        }

        void RecordSchedulingFailure(TPod* pod, const TError& error)
        {
            const auto& podId = pod->GetId();
            auto now = TInstant::Now();
            auto deadline = now + Owner_->Config_->FailedAllocationBackoffTime;
            YT_LOG_DEBUG(error, "Pod scheduling failure; backing off (PodId: %v, Deadline: %v)",
                podId,
                deadline);
            Owner_->ScheduleQueue_.Enqueue(podId, deadline);
            AllocationPlan_.RecordFailure(pod, error);
        }

        void SchedulePods()
        {
            YT_LOG_DEBUG("Started scheduling pods");

            auto now = TInstant::Now();
            while (true) {
                auto podId = Owner_->ScheduleQueue_.Dequeue(now);
                if (!podId) {
                    break;
                }

                auto* pod = Owner_->Cluster_->FindPod(podId);
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

                auto nodeOrError = Owner_->GlobalResourceAllocator_->ComputeAllocation(pod);
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
                for (int index = 0; index < Owner_->Config_->AllocationCommitConcurrency; ++index) {
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
                    MakeFormattableRange(perNodePlan.Requests, [] (auto* builder, const auto& request) {
                        FormatValue(builder, request, {});
                    }));

                try {
                    const auto& transactionManager = Owner_->Bootstrap_->GetTransactionManager();
                    auto transaction = WaitFor(transactionManager->StartReadWriteTransaction())
                        .ValueOrThrow();

                    auto* transactionNode = transaction->GetNode(perNodePlan.Node->GetId());
                    const auto& resourceManager = Owner_->Bootstrap_->GetResourceManager();
                    const auto& netManager = Owner_->Bootstrap_->GetNetManager();

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
                            Y_UNREACHABLE();
                        }
                    }

                    WaitFor(transaction->Commit())
                        .ThrowOnError();
                } catch (const std::exception& ex) {
                    auto now = TInstant::Now();
                    YT_LOG_DEBUG(ex, "Error committing pods assignment; will reschedule");
                    for (const auto& variantRequest : perNodePlan.Requests) {
                        if (auto request = std::get_if<TAllocationPlan::TPodRequest>(&variantRequest); request && request->Type == EAllocationPlanPodRequestType::AssignPodToNode) {
                            Owner_->ScheduleQueue_.Enqueue(request->Pod->GetId(), now);
                        }
                    }
                }
            }
        }

        void CommitSchedulingFailures()
        {
            const auto& failures = AllocationPlan_.GetFailures();

            try {
                const auto& transactionManager = Owner_->Bootstrap_->GetTransactionManager();
                auto transaction = WaitFor(transactionManager->StartReadWriteTransaction())
                    .ValueOrThrow();

                for (const auto& failure : failures) {
                    const auto& podId = failure.Pod->GetId();
                    auto* transactionPod = transaction->GetPod(podId);
                    if (!CheckPodSchedulable(transactionPod)) {
                        continue;
                    }

                    ToProto(transactionPod->Status().Other()->mutable_scheduling()->mutable_error(), failure.Error);
                }
                WaitFor(transaction->Commit())
                    .ThrowOnError();
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Error committing scheduling failures");
            }
        }
    };

    void OnSchedulingLoop()
    {
        try {
            New<TLoopIteration>(this)->Run();
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

const TClusterPtr& TScheduler::GetCluster() const
{
    return Impl_->GetCluster();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler

