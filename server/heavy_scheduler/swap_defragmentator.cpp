#include "swap_defragmentator.h"

#include "config.h"
#include "heavy_scheduler.h"
#include "helpers.h"
#include "private.h"
#include "resource_vector.h"
#include "task.h"
#include "task_manager.h"
#include "victim_set_generator.h"

#include <yp/server/lib/cluster/allocator.h>
#include <yp/server/lib/cluster/cluster.h>
#include <yp/server/lib/cluster/node.h>
#include <yp/server/lib/cluster/node_segment.h>
#include <yp/server/lib/cluster/pod.h>
#include <yp/server/lib/cluster/pod_disruption_budget.h>
#include <yp/server/lib/cluster/pod_set.h>

#include <yp/server/lib/objects/object_filter.h>

#include <yp/client/api/native/client.h>
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

class TSwapTask
    : public TTaskBase
{
public:
    TSwapTask(
        TGuid id,
        TInstant startTime,
        TObjectCompositeId starvingPodCompositeId,
        std::vector<TObjectCompositeId> victimPodCompositeIds)
        : TTaskBase(id, startTime)
        , StarvingPodCompositeId_(std::move(starvingPodCompositeId))
        , VictimPodCompositeIds_(std::move(victimPodCompositeIds))
    { }

    virtual std::vector<TObjectId> GetInvolvedPodIds() const override
    {
        std::vector<TObjectId> ids = {StarvingPodCompositeId_.Id};
        for (const auto& compositeId : VictimPodCompositeIds_) {
            ids.push_back(compositeId.Id);
        }
        return ids;
    }

    virtual void ReconcileState(const TClusterPtr& cluster) override
    {
        YT_VERIFY(State_ == ETaskState::Active);

        auto* starvingPod = FindPod(cluster, StarvingPodCompositeId_);

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

        std::vector<TObjectId> unevictedPods;
        for (const auto& victimPodCompositeId : VictimPodCompositeIds_) {
            auto* victimPod = FindPod(cluster, victimPodCompositeId);
            if (victimPod && victimPod->Eviction().state() != NProto::EEvictionState::ES_NONE) {
                unevictedPods.push_back(victimPodCompositeId.Id);
            }
        }

        if (!unevictedPods.empty()) {
            YT_LOG_DEBUG(
                "Swap task is considered not finished: some of victim pods are not evicted yet "
                "(UnevictedPodIds: %v)",
                unevictedPods);
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

            auto errorDatetime = error.GetDatetime();
            if (errorDatetime > LastErrorDatetime) {
                ++ErrorIterationCount;
            }
            LastErrorDatetime = errorDatetime;
        }
    };

    const TObjectCompositeId StarvingPodCompositeId_;
    const std::vector<TObjectCompositeId> VictimPodCompositeIds_;

    TSchedulingStatusSketch SchedulingStatusSketchAfterVictimEviction_;
};

////////////////////////////////////////////////////////////////////////////////

ITaskPtr CreateSwapTask(
    const IClientPtr& client,
    TPod* starvingPod,
    const std::vector<TPod*>& victimPods,
    bool validateDisruptionBudget)
{
    auto id = TGuid::Create();
    auto starvingPodCompositeId = GetCompositeId(starvingPod);

    std::vector<TObjectCompositeId> victimPodCompositeIds;
    for (auto* pod : victimPods) {
        victimPodCompositeIds.push_back(GetCompositeId(pod));
    }

    YT_LOG_DEBUG("Creating swap task (TaskId: %v, StarvingPod: %v, VictimPods: %v)",
        id,
        starvingPodCompositeId,
        victimPodCompositeIds);

    auto transactionId = WaitFor(client->StartTransaction())
        .ValueOrThrow()
        .TransactionId;

    for (auto* victimPod : victimPods) {
        WaitFor(RequestPodEviction(
            client,
            victimPod->GetId(),
            Format("Heavy Scheduler cluster defragmentation (TaskId: %v)", id),
            TRequestPodEvictionOptions{
                validateDisruptionBudget,
                .Reason = EEvictionReason::Scheduler},
            transactionId))
            .ValueOrThrow();
    }

    WaitFor(client->CommitTransaction(transactionId)).ThrowOnError();

    return New<TSwapTask>(
        std::move(id),
        TInstant::Now(),
        std::move(starvingPodCompositeId),
        std::move(victimPodCompositeIds));
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

    void Run(const TClusterPtr& cluster)
    {
        try {
            GuardedRun(cluster);
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error running swap defragmentator");
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

    void GuardedRun(const TClusterPtr& cluster)
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
                ProcessStarvingPod(*podIt);
            }
        }
    }

    void ProcessStarvingPod(TPod* starvingPod)
    {
        const auto& starvingPodFilteredNodesOrError = GetFilteredNodes(starvingPod);
        if (!starvingPodFilteredNodesOrError.IsOK()) {
            YT_LOG_DEBUG(starvingPodFilteredNodesOrError,
                "Error filltering starving pod suitable nodes (StarvingPodId: %v)",
                starvingPod->GetId());
            return;
        }
        auto starvingPodFilteredNodes = std::move(starvingPodFilteredNodesOrError.Value());

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

        Shuffle(starvingPodFilteredNodes.begin(), starvingPodFilteredNodes.end());
        if (static_cast<int>(starvingPodFilteredNodes.size()) > Config_->VictimNodeCandidateCount) {
            YT_LOG_DEBUG("Randomly selecting victim nodes (TotalCount: %v, RandomSelectionCount: %v)",
                starvingPodFilteredNodes.size(),
                Config_->VictimNodeCandidateCount);
            starvingPodFilteredNodes.resize(Config_->VictimNodeCandidateCount);
        }

        struct TSearchResult
        {
            std::vector<TPod*> VictimPods;
            double Score;
        };
        std::optional<TSearchResult> bestResult;

        for (auto* victimNode : starvingPodFilteredNodes) {
            if (!victimNode->CanAllocateAntiaffinityVacancies(starvingPod)) {
                YT_LOG_DEBUG_IF(HeavyScheduler_->GetVerbose(),
                    "Not enough antiaffinity vacancies (NodeId: %v, StarvingPodId: %v)",
                    victimNode->GetId(),
                    starvingPod->GetId());
                continue;
            }

            auto generator = CreateNodeVictimSetGenerator(
                Config_->VictimSetGeneratorType,
                victimNode,
                starvingPod,
                HeavyScheduler_->GetDisruptionThrottler(),
                HeavyScheduler_->GetVerbose());

            std::vector<TPod*> victimPods;
            while (!(victimPods = generator->GetNextCandidates()).empty()) {
                double score = GetVictimSetScore(victimPods);
                if (!bestResult || bestResult->Score < score) {
                    bestResult = {std::move(victimPods), score};
                }
            }
        }

        if (bestResult) {
            const auto& taskManager = HeavyScheduler_->GetTaskManager();
            if (taskManager->GetTaskSlotCount(ETaskSource::SwapDefragmentator) > 0) {
                auto task = CreateSwapTask(
                    HeavyScheduler_->GetClient(),
                    starvingPod,
                    bestResult->VictimPods,
                    HeavyScheduler_->GetDisruptionThrottler()->GetValidatePodDisruptionBudget());
                taskManager->Add(std::move(task), ETaskSource::SwapDefragmentator);
                HeavyScheduler_->GetDisruptionThrottler()->RegisterEviction(bestResult->VictimPods);
            } else {
                std::vector<TObjectId> victimPodIds;
                for (auto* pod : bestResult->VictimPods) {
                    victimPodIds.push_back(pod->GetId());
                }
                YT_LOG_DEBUG(
                    "Failed to create swap task: concurrent task limit reached for swap defragmentator "
                    "(StarvingPodId: %v, VictimPodIds: %v)",
                    starvingPod->GetId(),
                    victimPodIds);
            }
        } else {
            YT_LOG_DEBUG("Could not find victim pods (StarvingPodId: %v)",
                starvingPod->GetId());
            ++VictimSearchFailureCounter_;
        }
    }

    double GetVictimSetScore(const std::vector<TPod*>& victimPods)
    {
        return -victimPods.size();
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
};

////////////////////////////////////////////////////////////////////////////////

TSwapDefragmentator::TSwapDefragmentator(
    THeavyScheduler* heavyScheduler,
    TSwapDefragmentatorConfigPtr config)
    : Impl_(New<TImpl>(heavyScheduler, std::move(config)))
{ }

TSwapDefragmentator::~TSwapDefragmentator()
{ }

void TSwapDefragmentator::Run(const TClusterPtr& cluster)
{
    Impl_->Run(cluster);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
