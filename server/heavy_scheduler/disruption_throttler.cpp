#include "disruption_throttler.h"

#include "private.h"
#include "config.h"

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
        TDisruptionThrottlerConfigPtr config,
        bool verbose)
        : Config_(std::move(config))
        , Verbose_(verbose)
    { }

    void ReconcileState(const TClusterPtr& cluster)
    {
        YT_LOG_DEBUG_IF(Verbose_,
            "Reading evictions from cluster snapshot");
        EvictingPodIds_.clear();
        PodSetEvictionCount_.clear();
        for (auto* pod : cluster->GetSchedulablePods()) {
            if (pod->Eviction().state() != NProto::EEvictionState::ES_NONE) {
                YT_LOG_DEBUG_IF(Verbose_,
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
        YT_LOG_DEBUG_IF(Verbose_,
            "Registering pod eviction (PodId: %v, PodSetId: %v)",
            pod->GetId(),
            pod->PodSetId());
        YT_VERIFY(EvictingPodIds_.insert(pod->GetId()).second);
        PodSetEvictionCount_[pod->PodSetId()] += 1;
    }

    bool ThrottleEviction(TPod* pod) const
    {
        YT_VERIFY(pod->GetNode());
        YT_VERIFY(pod->GetEnableScheduling());

        if (IsBeingEvicted(pod->GetId())) {
            YT_LOG_DEBUG_IF(Verbose_,
                "Eviction throttled because pod is already scheduled for eviction (PodId: %v)",
                pod->GetId());
            return true;
        }

        if (auto error = pod->GetSchedulingAttributesValidationError(); !error.IsOK()) {
            YT_LOG_DEBUG_IF(Verbose_,
                "Cannot safely evict pod due to scheduling attributes validation error (PodId: %v, Error: %v)",
                pod->GetId(),
                error);
            return true;
        }

        auto evictionCountIt = PodSetEvictionCount_.find(pod->PodSetId());
        int evictionCount = evictionCountIt == PodSetEvictionCount_.end()
            ? 0
            : evictionCountIt->second;

        if (Config_->LimitEvictionsByPodSet) {
            if (evictionCount > 0) {
                YT_LOG_DEBUG_IF(Verbose_,
                    "Eviction throttled because another pod in the same pod set is being evicted (PodId: %v, PodSetId: %v)",
                    pod->GetId(),
                    pod->PodSetId());
                return true;
            }
        }

        if (Config_->ValidatePodDisruptionBudget) {
            if (const auto* podDisruptionBudget = pod->GetPodSet()->GetPodDisruptionBudget()) {
                int allowedPodDisruptions = podDisruptionBudget->Status().allowed_pod_disruptions();
                if (evictionCount >= allowedPodDisruptions) {
                    YT_LOG_DEBUG_IF(Verbose_,
                        "Eviction throttled because of pod disruption budget (PodId: %v, DisruptionCount: %v, AllowedDisruptionCount: %v)",
                        pod->GetId(),
                        evictionCount,
                        allowedPodDisruptions);
                    return true;
                }
            } else {
                YT_LOG_DEBUG_IF(Verbose_,
                    "Eviction throttled because pod is not attached to a disruption budget (PodId: %v)",
                    pod->GetId());
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

private:
    const TDisruptionThrottlerConfigPtr Config_;
    const bool Verbose_;

    THashSet<TObjectId> EvictingPodIds_;
    THashMap<TObjectId, int> PodSetEvictionCount_;
};

////////////////////////////////////////////////////////////////////////////////

TDisruptionThrottler::TDisruptionThrottler(
    TDisruptionThrottlerConfigPtr config,
    bool verbose)
    : Impl_(New<TImpl>(std::move(config), verbose))
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
