#include "swap_defragmentator.h"

#include "config.h"
#include "heavy_scheduler.h"
#include "helpers.h"
#include "private.h"
#include "resource_vector.h"
#include "task.h"
#include "task_manager.h"

#include <yp/server/lib/cluster/allocator.h>
#include <yp/server/lib/cluster/cluster.h>
#include <yp/server/lib/cluster/node.h>
#include <yp/server/lib/cluster/node_segment.h>
#include <yp/server/lib/cluster/pod.h>
#include <yp/server/lib/cluster/pod_disruption_budget.h>
#include <yp/server/lib/cluster/pod_set.h>

#include <yp/server/lib/objects/object_filter.h>

#include <yp/client/api/native/helpers.h>

#include <yt/core/misc/finally.h>

#include <util/random/shuffle.h>

namespace NYP::NServer::NHeavyScheduler {

using namespace NCluster;

using namespace NClient::NApi;
using namespace NClient::NApi::NNative;

using namespace NConcurrency;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

TInstant ParseErrorDatetime(const TError& error)
{
    return TInstant::ParseIso8601(error.Attributes().Get<TString>("datetime"));
}

////////////////////////////////////////////////////////////////////////////////

class TSwapTask
    : public TTaskBase
{
public:
    TSwapTask(
        TGuid id,
        TInstant startTime,
        TObjectCompositeId starvingPodCompositeId,
        TObjectCompositeId victimPodCompositeId)
        : TTaskBase(id, startTime)
        , StarvingPodCompositeId_(std::move(starvingPodCompositeId))
        , VictimPodCompositeId_(std::move(victimPodCompositeId))
    { }

    virtual std::vector<TObjectId> GetInvolvedPodIds() const override
    {
        return {StarvingPodCompositeId_.Id, VictimPodCompositeId_.Id};
    }

    virtual void ReconcileState(const TClusterPtr& cluster) override
    {
        YT_VERIFY(State_ == ETaskState::Active);

        auto* starvingPod = FindPod(cluster, StarvingPodCompositeId_);
        auto* victimPod = FindPod(cluster, VictimPodCompositeId_);

        if (!starvingPod) {
            YT_LOG_DEBUG("Swap task is considered finished; starving pod does not exist");
            State_ = ETaskState::Succeeded;
            return;
        }

        if (starvingPod->GetNode()) {
            YT_LOG_DEBUG("Swap task is considered finished; starving pod is scheduled");
            State_ = ETaskState::Succeeded;
            return;
        }

        if (victimPod && victimPod->Eviction().state() != NProto::EEvictionState::ES_NONE) {
            YT_LOG_DEBUG("Swap task is considered not finished; victim pod is not evicted yet");
            return;
        }

        SchedulingStatusSketchAfterVictimEviction_.Update(starvingPod);

        // Ensure at least one scheduling iteration after victim eviction.
        if (SchedulingStatusSketchAfterVictimEviction_.ErrorIterationCount >= 3) {
            YT_LOG_DEBUG(
                "Swap task is considered finished; "
                "passed at least one scheduling iteration after victim eviction");
            State_ = ETaskState::Failed;
        } else {
            YT_LOG_DEBUG(
                "Swap task is cosidered not finished; "
                "no evidence of passed scheduling iteration after victim eviction");
        }
    }

private:
    struct TSchedulingStatusSketch
    {
        int ErrorIterationCount = 0;
        TInstant LastErrorDatetime = TInstant::Zero();

        void Update(const TPod* pod)
        {
            auto error = pod->ParseSchedulingError();
            if (error.IsOK()) {
                return;
            }

            auto errorDatetime = ParseErrorDatetime(error);
            if (errorDatetime > LastErrorDatetime) {
                ++ErrorIterationCount;
            }
            LastErrorDatetime = errorDatetime;
        }
    };

    const TObjectCompositeId StarvingPodCompositeId_;
    const TObjectCompositeId VictimPodCompositeId_;

    TSchedulingStatusSketch SchedulingStatusSketchAfterVictimEviction_;
};

////////////////////////////////////////////////////////////////////////////////

ITaskPtr CreateSwapTask(const IClientPtr& client, TPod* starvingPod, TPod* victimPod)
{
    auto id = TGuid::Create();
    auto starvingPodCompositeId = GetCompositeId(starvingPod);
    auto victimPodCompositeId = GetCompositeId(victimPod);

    YT_LOG_DEBUG("Creating swap task (TaskId: %v, StarvingPod: %v, VictimPod: %v)",
        id,
        starvingPodCompositeId,
        victimPodCompositeId);

    WaitFor(RequestPodEviction(
        client,
        victimPod->GetId(),
        Format("Heavy Scheduler cluster defragmentation (TaskId: %v)", id),
        TRequestPodEvictionOptions{
            .ValidateDisruptionBudget = true,
            .Reason = EEvictionReason::Scheduler}))
        .ValueOrThrow();

    return New<TSwapTask>(
        std::move(id),
        TInstant::Now(),
        std::move(starvingPodCompositeId),
        std::move(victimPodCompositeId));
}

////////////////////////////////////////////////////////////////////////////////

class TSwapDefragmentator::TImpl
    : public TRefCounted
{
public:
    TImpl(
        THeavyScheduler* heavyScheduler,
        TSwapDefragmentatorConfigPtr config)
        : HeavyScheduler_(heavyScheduler)
        , Config_(std::move(config))
    { }

    void CreateTasks(const TClusterPtr& cluster)
    {
        auto starvingPods = FindStarvingPods(cluster);
        if (starvingPods.empty()) {
            YT_LOG_DEBUG("There are no starving pods; skipping iteration");
            return;
        }

        Shuffle(starvingPods.begin(), starvingPods.end());

        auto podItEnd = static_cast<int>(starvingPods.size()) > Config_->StarvingPodsPerIterationLimit
            ? starvingPods.begin() + Config_->StarvingPodsPerIterationLimit
            : starvingPods.end();

        VictimSearchFailureCounter_ = 0;
        auto finally = Finally([this] () {
            Profiler.Update(Profiling_.VictimSearchFailureCounter, VictimSearchFailureCounter_);
        });

        for (auto podIt = starvingPods.begin(); podIt < podItEnd; ++podIt) {
            if (!HeavyScheduler_->GetTaskManager()->HasTaskInvolvingPod(*podIt)) {
                TryCreateSwapTask(cluster, *podIt);
            }
        }
    }

private:
    THeavyScheduler* const HeavyScheduler_;
    const TSwapDefragmentatorConfigPtr Config_;

    int VictimSearchFailureCounter_;

    struct TProfiling
    {
        NProfiling::TSimpleGauge VictimSearchFailureCounter{"/victim_search_failure"};
    };

    TProfiling Profiling_;

    void TryCreateSwapTask(
        const TClusterPtr& cluster,
        TPod* starvingPod)
    {
        const auto& starvingPodFilteredNodesOrError = GetFilteredNodes(starvingPod);
        if (!starvingPodFilteredNodesOrError.IsOK()) {
            YT_LOG_DEBUG(starvingPodFilteredNodesOrError,
                "Error filltering starving pod suitable nodes (StarvingPodId: %v)",
                starvingPod->GetId());
            return;
        }
        const auto& starvingPodFilteredNodes = starvingPodFilteredNodesOrError.Value();

        auto starvingPodSuitableNodes = FindSuitableNodes(
            starvingPod,
            starvingPodFilteredNodes,
            /* limit */ 1);
        if (starvingPodSuitableNodes.size() > 0) {
            YT_LOG_DEBUG("Found suitable node for starving pod (PodId: %v, NodeId: %v)",
                starvingPod->GetId(),
                starvingPodSuitableNodes[0]->GetId());
            return;
        }

        const auto& taskManager = HeavyScheduler_->GetTaskManager();

        int minSuitableNodeCount = Config_->SafeSuitableNodeCount + taskManager->TaskCount();
        auto* victimPod = FindVictimPod(
            cluster,
            starvingPod,
            starvingPodFilteredNodes,
            minSuitableNodeCount);
        if (!victimPod) {
            YT_LOG_DEBUG("Could not find victim pod (StarvingPodId: %v)",
                starvingPod->GetId());
            ++VictimSearchFailureCounter_;
            return;
        }

        YT_LOG_DEBUG("Found victim pod (PodId: %v, StarvingPodId: %v)",
            victimPod->GetId(),
            starvingPod->GetId());

        if (taskManager->GetTaskSlotCount(ETaskSource::SwapDefragmentator) > 0) {
            auto task = CreateSwapTask(
                HeavyScheduler_->GetClient(),
                starvingPod,
                victimPod);
            taskManager->Add(task, ETaskSource::SwapDefragmentator);
            HeavyScheduler_->GetDisruptionThrottler()->RegisterPodEviction(victimPod);
        } else {
            YT_LOG_DEBUG("Failed to create swap task: concurrent task limit reached for swap defragmentator "
                "(VictimPodId: %v, StarvingPodId: %v)",
                victimPod->GetId(),
                starvingPod->GetId());
        }
    }

    std::vector<TPod*> FindStarvingPods(const TClusterPtr& cluster) const
    {
        std::vector<TPod*> result;
        for (auto* pod : GetNodeSegmentSchedulablePods(cluster, HeavyScheduler_->GetNodeSegment())) {
            if (pod->GetNode()) {
                continue;
            }
            if (!pod->ParseSchedulingError().IsOK()) {
                result.push_back(pod);
            }
        }
        YT_LOG_DEBUG_UNLESS(result.empty(), "Found starving pods (Count: %v)",
            result.size());
        return result;
    }

    TPod* FindVictimPod(
        const TClusterPtr& cluster,
        TPod* starvingPod,
        const std::vector<TNode*>& starvingPodFilteredNodes,
        int minSuitableNodeCount) const
    {
        THashSet<TNode*> starvingPodFilteredNodeSet;
        for (auto* node : starvingPodFilteredNodes) {
            starvingPodFilteredNodeSet.insert(node);
        }

        std::vector<TPod*> victimCandidatePods = GetNodeSegmentSchedulablePods(
            cluster,
            HeavyScheduler_->GetNodeSegment());
        victimCandidatePods.erase(
            std::remove_if(
                victimCandidatePods.begin(),
                victimCandidatePods.end(),
                [&] (TPod* pod) {
                    return !pod->GetNode() ||
                        starvingPodFilteredNodeSet.find(pod->GetNode()) == starvingPodFilteredNodeSet.end();
                }),
            victimCandidatePods.end());

        if (static_cast<int>(victimCandidatePods.size()) > Config_->VictimCandidatePodCount) {
            YT_LOG_DEBUG("Randomly selecting victim candidates (TotalCount: %v, RandomSelectionCount: %v)",
                victimCandidatePods.size(),
                Config_->VictimCandidatePodCount);
            Shuffle(victimCandidatePods.begin(), victimCandidatePods.end());
            victimCandidatePods.resize(Config_->VictimCandidatePodCount);
        }

        YT_LOG_DEBUG("Selected victim pod candidates (Count: %v)",
            victimCandidatePods.size());

        for (auto* victimPod : victimCandidatePods) {
            auto* node = victimPod->GetNode();

            if (!node->CanAllocateAntiaffinityVacancies(starvingPod)) {
                YT_LOG_DEBUG_IF(HeavyScheduler_->GetVerbose(),
                    "Not enough antiaffinity vacancies (NodeId: %v, StarvingPodId: %v)",
                    node->GetId(),
                    starvingPod->GetId());
                continue;
            }

            auto starvingPodResourceVector = GetResourceRequestVector(starvingPod);
            auto victimPodResourceVector = GetResourceRequestVector(victimPod);
            auto freeNodeResourceVector = GetFreeResourceVector(node);
            if (freeNodeResourceVector + victimPodResourceVector < starvingPodResourceVector) {
                YT_LOG_DEBUG_IF(HeavyScheduler_->GetVerbose(),
                    "Not enough resources according to resource vectors (NodeId: %v, VictimPodId: %v, StarvingPodId: %v)",
                    node->GetId(),
                    victimPod->GetId(),
                    starvingPod->GetId());
                continue;
            }

            YT_LOG_DEBUG_IF(HeavyScheduler_->GetVerbose(),
                "Checking eviction safety (PodId: %v)",
                victimPod->GetId());
            if (HeavyScheduler_->GetDisruptionThrottler()->ThrottleEviction(victimPod)
                || !HasEnoughSuitableNodes(victimPod, minSuitableNodeCount, HeavyScheduler_->GetVerbose()))
            {
                continue;
            }

            return victimPod;
        }

        return nullptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

TSwapDefragmentator::TSwapDefragmentator(
    THeavyScheduler* heavyScheduler,
    TSwapDefragmentatorConfigPtr config)
    : Impl_(New<TImpl>(heavyScheduler, std::move(config)))
{ }

void TSwapDefragmentator::CreateTasks(const TClusterPtr& cluster)
{
    Impl_->CreateTasks(cluster);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
