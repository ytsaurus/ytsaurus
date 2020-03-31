#include "disruption_throttler.h"

#include "config.h"
#include "heavy_scheduler.h"
#include "helpers.h"
#include "private.h"
#include "task_manager.h"

#include <yp/server/lib/cluster/cluster.h>
#include <yp/server/lib/cluster/pod.h>
#include <yp/server/lib/cluster/pod_set.h>
#include <yp/server/lib/cluster/pod_disruption_budget.h>

namespace NYP::NServer::NHeavyScheduler {

using namespace NCluster;

using namespace NClient::NApi;

////////////////////////////////////////////////////////////////////////////////

class TDisruptionThrottler::TImpl
    : public TRefCounted
{
public:
    TImpl(
        THeavyScheduler* heavyScheduler,
        TDisruptionThrottlerConfigPtr config)
        : HeavyScheduler_(heavyScheduler)
        , Config_(std::move(config))
    { }

    void ReconcileState(const TClusterPtr& cluster)
    {
        YT_LOG_DEBUG_IF(HeavyScheduler_->GetVerbose(),
            "Reading evictions from cluster snapshot");
        EvictingPodIds_.clear();
        PodSetEvictionCount_.clear();
        PodSetAddedEvictionCount_.clear();
        SuitableNodeCountCache_.clear();
        for (auto* pod : cluster->GetSchedulablePods()) {
            if (pod->Eviction().state() != NProto::EEvictionState::ES_NONE) {
                YT_LOG_DEBUG_IF(HeavyScheduler_->GetVerbose(),
                    "Read pod eviction (PodId: %v, PodSetId: %v)",
                    pod->GetId(),
                    pod->PodSetId());
                YT_VERIFY(EvictingPodIds_.insert(pod->GetId()).second);
                PodSetEvictionCount_[pod->PodSetId()] += 1;
            }
        }
    }

    void RegisterEviction(TPod* pod)
    {
        YT_LOG_DEBUG_IF(HeavyScheduler_->GetVerbose(),
            "Registering pod eviction (PodId: %v, PodSetId: %v)",
            pod->GetId(),
            pod->PodSetId());
        YT_VERIFY(EvictingPodIds_.insert(pod->GetId()).second);
        PodSetEvictionCount_[pod->PodSetId()] += 1;
        PodSetAddedEvictionCount_[pod->PodSetId()] += 1;
    }

    bool ThrottleEviction(TPod* pod) const
    {
        return ThrottleEvictionImpl(pod,
            /* samePodSetConcurrentEvictionsCount = */ 0,
            /* totalConcurrentEvictionsCount = */ 0);
    }

    bool ThrottleEviction(const std::vector<TPod*>& pods) const
    {
        THashMap<TPodSet*, int> podSetEvictionCount;
        for (auto* pod : pods) {
            podSetEvictionCount[pod->GetPodSet()] += 1;
        }

        int totalConcurrentEvictionsCount = static_cast<int>(pods.size()) - 1;
        for (auto* pod : pods) {
            int samePodSetConcurrentEvictionsCount = podSetEvictionCount[pod->GetPodSet()] - 1;
            if (ThrottleEvictionImpl(pod,
                samePodSetConcurrentEvictionsCount,
                totalConcurrentEvictionsCount))
            {
                return true;
            }
        }
        return false;
    }

    int EvictionCount() const
    {
        return EvictingPodIds_.size();
    }

    bool IsBeingEvicted(const TObjectId& podId) const
    {
        return EvictingPodIds_.find(podId) != EvictingPodIds_.end();
    }

    bool GetValidatePodDisruptionBudget() const
    {
        return Config_->ValidatePodDisruptionBudget;
    }

private:
    THeavyScheduler* const HeavyScheduler_;
    const TDisruptionThrottlerConfigPtr Config_;

    THashSet<TObjectId> EvictingPodIds_;
    THashMap<TObjectId, int> PodSetEvictionCount_;
    THashMap<TObjectId, int> PodSetAddedEvictionCount_;

    mutable THashMap<TObjectId, TErrorOr<int>> SuitableNodeCountCache_;

    bool ThrottleEvictionImpl(
        TPod* pod,
        int samePodSetConcurrentEvictionsCount,
        int totalConcurrentEvictionsCount) const
    {
        YT_VERIFY(pod->GetNode());
        YT_VERIFY(pod->GetEnableScheduling());

        if (IsBeingEvicted(pod->GetId())) {
            YT_LOG_DEBUG_IF(HeavyScheduler_->GetVerbose(),
                "Eviction throttled because pod is already scheduled for eviction (PodId: %v)",
                pod->GetId());
            return true;
        }

        if (auto error = pod->GetSchedulingAttributesValidationError(); !error.IsOK()) {
            YT_LOG_DEBUG_IF(HeavyScheduler_->GetVerbose(),
                "Cannot safely evict pod due to scheduling attributes validation error (PodId: %v, Error: %v)",
                pod->GetId(),
                error);
            return true;
        }

        auto evictionCountIt = PodSetEvictionCount_.find(pod->PodSetId());
        int evictionCount = evictionCountIt == PodSetEvictionCount_.end()
            ? 0
            : evictionCountIt->second;

        if (Config_->LimitEvictionsByPodSet
            && (evictionCount > 0 || samePodSetConcurrentEvictionsCount > 0))
        {
            YT_LOG_DEBUG_IF(HeavyScheduler_->GetVerbose(),
                "Eviction throttled because another pod in the same pod set is being evicted (PodId: %v, PodSetId: %v)",
                pod->GetId(),
                pod->PodSetId());
            return true;
        }

        if (Config_->ValidatePodDisruptionBudget) {
            if (const auto* podDisruptionBudget = pod->GetPodSet()->GetPodDisruptionBudget()) {
                auto addedEvictionCountIt = PodSetAddedEvictionCount_.find(pod->PodSetId());
                int addedEvictionCount = (addedEvictionCountIt == PodSetAddedEvictionCount_.end()
                    ? 0
                    : addedEvictionCountIt->second)
                    + samePodSetConcurrentEvictionsCount;
                int allowedPodDisruptions = podDisruptionBudget->Status().allowed_pod_disruptions();
                if (addedEvictionCount >= allowedPodDisruptions) {
                    YT_LOG_DEBUG_IF(HeavyScheduler_->GetVerbose(),
                        "Eviction throttled because of pod disruption budget (PodId: %v, DisruptionCount: %v, AllowedDisruptionCount: %v)",
                        pod->GetId(),
                        addedEvictionCount,
                        allowedPodDisruptions);
                    return true;
                }
            } else {
                YT_LOG_DEBUG_IF(HeavyScheduler_->GetVerbose(),
                    "Eviction throttled because pod is not attached to a disruption budget (PodId: %v)",
                    pod->GetId());
                return true;
            }
        }

        int minSuitableNodeCount = Config_->SafeSuitableNodeCount
            + HeavyScheduler_->GetTaskManager()->TaskCount()
            + totalConcurrentEvictionsCount;
        auto suitableNodeCountOrError = GetSuitableNodeCount(pod);
        if (!suitableNodeCountOrError.IsOK()) {
            YT_LOG_DEBUG_IF(HeavyScheduler_->GetVerbose(),
                suitableNodeCountOrError,
                "Error finding suitable nodes (PodId: %v)",
                pod->GetId());
            return true;
        }

        int suitableNodeCount = suitableNodeCountOrError.Value();
        if (suitableNodeCount < minSuitableNodeCount) {
            YT_LOG_DEBUG_IF(HeavyScheduler_->GetVerbose(),
                "Eviction throttled because pod does not have enough suitable nodes "
                "(PodId: %v, SuitableNodeCount: %v, MinSuitableNodeCount: %v)",
                pod->GetId(), suitableNodeCount, minSuitableNodeCount);
            return true;
        }

        return false;
    }

    TErrorOr<int> GetSuitableNodeCount(TPod* pod) const
    {
        if (auto it = SuitableNodeCountCache_.find(pod->GetId());
            it != SuitableNodeCountCache_.end())
        {
            return it->second;
        }

        auto suitableNodesOrError = FindSuitableNodes(pod);
        if (!suitableNodesOrError.IsOK()) {
            auto error = static_cast<TError>(suitableNodesOrError);
            YT_VERIFY(SuitableNodeCountCache_.insert({pod->GetId(), error}).second);
            return error;
        }

        int count = suitableNodesOrError.Value().size();
        YT_VERIFY(SuitableNodeCountCache_.insert({pod->GetId(), count}).second);

        YT_LOG_DEBUG_IF(HeavyScheduler_->GetVerbose(),
            "Found suitable nodes (PodId: %v, SuitableNodeCount: %v)",
            pod->GetId(),
            count);

        return count;
    }
};

////////////////////////////////////////////////////////////////////////////////

TDisruptionThrottler::TDisruptionThrottler(
    THeavyScheduler* heavyScheduler,
    TDisruptionThrottlerConfigPtr config)
    : Impl_(New<TImpl>(heavyScheduler, std::move(config)))
{ }

TDisruptionThrottler::~TDisruptionThrottler()
{ }

void TDisruptionThrottler::ReconcileState(const TClusterPtr& cluster)
{
    Impl_->ReconcileState(cluster);
}

void TDisruptionThrottler::RegisterEviction(TPod* pod)
{
    Impl_->RegisterEviction(pod);
}

void TDisruptionThrottler::RegisterEviction(const std::vector<TPod*>& pods)
{
    for (auto* pod : pods) {
        Impl_->RegisterEviction(pod);
    }
}

bool TDisruptionThrottler::ThrottleEviction(TPod* pod) const
{
    return Impl_->ThrottleEviction(pod);
}

bool TDisruptionThrottler::ThrottleEviction(const std::vector<TPod*>& pods) const
{
    return Impl_->ThrottleEviction(pods);
}

int TDisruptionThrottler::EvictionCount() const
{
    return Impl_->EvictionCount();
}

bool TDisruptionThrottler::IsBeingEvicted(const TObjectId& podId) const
{
    return Impl_->IsBeingEvicted(podId);
}

bool TDisruptionThrottler::GetValidatePodDisruptionBudget() const
{
    return Impl_->GetValidatePodDisruptionBudget();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
