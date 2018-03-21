#include "scheduler.h"
#include "config.h"
#include "cluster.h"
#include "pod.h"
#include "node.h"
#include "schedule_queue.h"
#include "allocation_plan.h"
#include "helpers.h"
#include "resource_manager.h"
#include "private.h"

#include <yp/server/master/yt_connector.h>
#include <yp/server/master/bootstrap.h>

#include <yp/server/objects/transaction_manager.h>
#include <yp/server/objects/transaction.h>
#include <yp/server/objects/pod.h>
#include <yp/server/objects/node.h>

#include <yp/server/net/net_manager.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/periodic_executor.h>

namespace NYP {
namespace NServer {
namespace NScheduler {

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
    {
        VERIFY_INVOKER_THREAD_AFFINITY(LoopActionQueue_->GetInvoker(), LoopThread);
    }

    void Initialize()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        ytConnector->SubscribeStartedLeading(BIND(&TImpl::OnStartedLeading, MakeWeak(this)));
        ytConnector->SubscribeStoppedLeading(BIND(&TImpl::OnStoppedLeading, MakeWeak(this)));
    }

private:
    TBootstrap* const Bootstrap_;
    const TSchedulerConfigPtr Config_;

    const TActionQueuePtr LoopActionQueue_ = New<TActionQueue>("SchedulerLoop");
    const TPeriodicExecutorPtr SchedulingLoopExecutor_;

    TClusterPtr Cluster_;
    TScheduleQueue ScheduleQueue_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(LoopThread);


    class TLoopIteration
        : public TRefCounted
    {
    public:
        explicit TLoopIteration(TImplPtr owner)
            : Owner_(std::move(owner))
            , Allocator_(Owner_->Cluster_)
        { }

        void Run()
        {
            ReconcileState();
            RequestPodEvictionAtNodesWithRequestedMaintenance();
            RevokePodsWithAcknowledgedEviction();
            AcknowledgeNodeMaintenance();
            SchedulePods();
            Commit();
        }

    private:
        const TImplPtr Owner_;

        TGlobalResourceAllocator Allocator_;
        TAllocationPlan AllocationPlan_;


        void ReconcileState()
        {
            LOG_DEBUG("Started reconciling state");

            Owner_->Cluster_->LoadSnapshot();

            auto pods = Owner_->Cluster_->GetPods();
            auto now = TInstant::Now();
            for (auto* pod : pods) {
                if (!pod->GetNode()) {
                    LOG_DEBUG("Adding pod to schedule queue since it is not assigned to any node (PodId: %v)",
                        pod->GetId());
                    Owner_->ScheduleQueue_.Enqueue(pod->GetId(), now);
                }
            }

            AllocationPlan_.Clear();

            LOG_DEBUG("State reconciled");
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
                    LOG_DEBUG("Pod eviction requested by HFSM (PodId: %v, NodeId: %v, HfsmState: %v)",
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
                LOG_DEBUG(ex, "Error committing pod eviction state transitions");
            }
        }

        void RevokePodsWithAcknowledgedEviction()
        {
            auto pods = Owner_->Cluster_->GetPods();
            for (auto* pod : pods) {
                if (pod->StatusOther().eviction().state() == NClient::NApi::NProto::ES_ACKNOWLEDGED) {
                    LOG_DEBUG("Pod eviction acknowledged (PodId: %v, NodeId: %v)",
                        pod->GetId(),
                        pod->GetNode()->GetId());
                    AllocationPlan_.RevokePodFromNode(pod);
                }
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
                    LOG_DEBUG("Node maintenance acknowledged (NodeId: %v)",
                        node->GetId());
                    transactionNode->UpdateMaintenanceStatus(
                        ENodeMaintenanceState::Acknowledged,
                        "Maintenance acknowledged by scheduler since no pods are left on this node");
                }

                WaitFor(transaction->Commit())
                    .ThrowOnError();
            } catch (const std::exception& ex) {
                LOG_DEBUG(ex, "Error committing node maintenance state transitions");
            }
        }

        void RecordAllocationSuccess(TPod* pod, TNode* node)
        {
            LOG_DEBUG("Node allocation succeeded (PodId: %v, NodeId: %v)",
                pod->GetId(),
                node->GetId());
            AllocationPlan_.AssignPodToNode(pod, node);
        }

        void RecordAllocationFailure(TPod* pod)
        {
            const auto& podId = pod->GetId();
            auto now = TInstant::Now();
            auto deadline = now + Owner_->Config_->FailedAllocationBackoffTime;
            LOG_DEBUG("Node allocation failed; backing off (PodId: %v, Deadline: %v)",
                podId,
                deadline);
            Owner_->ScheduleQueue_.Enqueue(podId, deadline);
        }

        void RecordSchedulingFailure(TPod* pod, const TError& error)
        {
            const auto& podId = pod->GetId();
            auto now = TInstant::Now();
            auto deadline = now + Owner_->Config_->FailedAllocationBackoffTime;
            LOG_DEBUG(error, "Pod scheduling failure; backing off (PodId: %v, Deadline: %v)",
                podId,
                deadline);
            Owner_->ScheduleQueue_.Enqueue(podId, deadline);
            AllocationPlan_.RecordFailure(pod, error);
        }

        void SchedulePods()
        {
            LOG_DEBUG("Started scheduling pods");

            auto now = TInstant::Now();
            while (true) {
                auto podId = Owner_->ScheduleQueue_.Dequeue(now);
                if (!podId) {
                    break;
                }

                auto* pod = Owner_->Cluster_->FindPod(podId);
                if (!pod) {
                    LOG_DEBUG("Pod no longer exists; discarded (PodId: %v)",
                        podId);
                    continue;
                }

                if (pod->GetNode()) {
                    LOG_DEBUG("Pod is already assigned to node; discarded (PodId: %v, NodeId: %v)",
                        podId,
                        pod->GetNode()->GetId());
                    continue;
                }

                auto nodeOrError = Allocator_.ComputeAllocation(pod);
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
            LOG_DEBUG("Pods scheduled");
        }

        void Commit()
        {
            {
                LOG_DEBUG("Started committing scheduling results (PodCount: %v, NodeCount: %v, FailureCount: %v)",
                    AllocationPlan_.GetPodCount(),
                    AllocationPlan_.GetNodeCount(),
                    AllocationPlan_.GetFailures().size());

                std::vector<TFuture<void>> asyncResults;
                for (int index = 0; index < Owner_->Config_->AllocationCommitConcurrency; ++index) {
                    asyncResults.push_back(BIND(&TLoopIteration::CommitScheduledPods, MakeStrong(this))
                        .AsyncVia(GetCurrentInvoker())
                        .Run());
                }
                WaitFor(Combine(asyncResults))
                    .ThrowOnError();

                LOG_DEBUG("Scheduled pods committed");
            }

            if (!AllocationPlan_.GetFailures().empty()) {
                LOG_DEBUG("Started committing scheduling failures (Count: %v)",
                    AllocationPlan_.GetPodCount(),
                    AllocationPlan_.GetNodeCount(),
                    AllocationPlan_.GetFailures().size());

                WaitFor(BIND(&TLoopIteration::CommitSchedulingFailures, MakeStrong(this))
                    .AsyncVia(GetCurrentInvoker())
                    .Run())
                    .ThrowOnError();

                LOG_DEBUG("Scheduling failures committed");
            }
        }

        bool CheckPodSchedulable(NObjects::TPod* pod)
        {
            const auto& podId = pod->GetId();

            if (!pod->Exists()) {
                LOG_DEBUG("Pod no longer exists; discarded (PodId: %v)",
                    podId);
                return false;
            }

            if (!pod->Spec().EnableScheduling().Load()) {
                LOG_DEBUG("Pod scheduling disabled; discarded (PodId: %v)",
                    podId);
                return false;
            }

            return true;
        }

        void CommitScheduledPods()
        {
            while (true) {
                auto maybePerNodePlan = AllocationPlan_.TryExtractPerNodePlan();
                if (!maybePerNodePlan) {
                    break;
                }

                const auto& perNodePlan = *maybePerNodePlan;

                LOG_DEBUG("Committing pods assignment (NodeId: %v, PodIds: %v)",
                    perNodePlan.Node->GetId(),
                    MakeFormattableRange(perNodePlan.Requests, [] (auto* builder, const auto& request) {
                        builder->AppendFormat("%v%v",
                            request.Assign ? "+" : "-",
                            request.Pod->GetId());
                    }));

                try {
                    const auto& transactionManager = Owner_->Bootstrap_->GetTransactionManager();
                    auto transaction = WaitFor(transactionManager->StartReadWriteTransaction())
                        .ValueOrThrow();

                    auto* transactionNode = transaction->GetNode(perNodePlan.Node->GetId());
                    const auto& resourceManager = Owner_->Bootstrap_->GetResourceManager();
                    const auto& netManager = Owner_->Bootstrap_->GetNetManager();
                    for (const auto& request : perNodePlan.Requests) {
                        const auto& podId = request.Pod->GetId();
                        auto* transactionPod = transaction->GetPod(podId);

                        try {
                            if (!CheckPodSchedulable(transactionPod)) {
                                continue;
                            }

                            if (request.Assign) {
                                resourceManager->AssignPodToNode(transaction, transactionNode, transactionPod);
                            } else {
                                if (transactionPod->Spec().Node().Load() != transactionNode) {
                                    LOG_DEBUG("Pod is no longer assigned to the expected node; skipped (PodId: %v, ExpectedNodeId: %v, ActualNodeId: %v)",
                                        podId,
                                        perNodePlan.Node->GetId(),
                                        transactionPod->Spec().Node().Load()->GetId());
                                    continue;
                                }
                                resourceManager->RevokePodFromNode(transaction, transactionPod);
                            }

                            netManager->UpdatePodAddresses(transaction, transactionPod);
                        } catch (const std::exception& ex) {
                            auto error = TError("Error assigning pod to node %Qv",
                                transactionNode->GetId())
                                << ex;
                            RecordSchedulingFailure(request.Pod, error);
                        }
                    }

                    WaitFor(transaction->Commit())
                        .ThrowOnError();
                } catch (const std::exception& ex) {
                    auto now = TInstant::Now();
                    LOG_DEBUG(ex, "Error committing pods assignment; will reschedule");
                    for (const auto& request : perNodePlan.Requests) {
                        if (request.Assign) {
                            Owner_->ScheduleQueue_.Enqueue(request.Pod->GetId(), now);
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
                LOG_DEBUG(ex, "Error committing scheduling failures");
            }
        }
    };

    void OnSchedulingLoop()
    {
        try {
            New<TLoopIteration>(this)->Run();
        } catch (const std::exception& ex) {
            LOG_WARNING(ex, "Loop iteration failed");
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP

