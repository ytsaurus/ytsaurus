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

    void RegisterPodEviction(TPod* pod)
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

        if (Config_->LimitEvictionsByPodSet && evictionCount > 0) {
            YT_LOG_DEBUG_IF(HeavyScheduler_->GetVerbose(),
                "Eviction throttled because another pod in the same pod set is being evicted (PodId: %v, PodSetId: %v)",
                pod->GetId(),
                pod->PodSetId());
            return true;
        }

        if (Config_->ValidatePodDisruptionBudget) {
            if (const auto* podDisruptionBudget = pod->GetPodSet()->GetPodDisruptionBudget()) {
                auto addedEvictionCountIt = PodSetAddedEvictionCount_.find(pod->PodSetId());
                int addedEvictionCount = addedEvictionCountIt == PodSetAddedEvictionCount_.end()
                    ? 0
                    : addedEvictionCountIt->second;
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
            + HeavyScheduler_->GetTaskManager()->TaskCount();
        auto suitableNodeCountOrError = GetSuitableNodeCount(pod, minSuitableNodeCount);
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

    TErrorOr<int> GetSuitableNodeCount(TPod* pod, int limit) const
    {
        auto suitableNodesOrError = FindSuitableNodes(pod, limit);
        if (!suitableNodesOrError.IsOK()) {
            return static_cast<TError>(suitableNodesOrError);
        }
        const auto& suitableNodes = suitableNodesOrError.Value();

        YT_LOG_DEBUG_IF(HeavyScheduler_->GetVerbose(),
            "Found suitable nodes (PodId: %v, SuitableNodeCount: %v)",
            pod->GetId(),
            suitableNodes.size());

        return suitableNodes.size();
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

void TDisruptionThrottler::RegisterPodEviction(TPod *pod)
{
    Impl_->RegisterPodEviction(pod);
}

bool TDisruptionThrottler::ThrottleEviction(TPod* pod) const
{
    return Impl_->ThrottleEviction(pod);
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
