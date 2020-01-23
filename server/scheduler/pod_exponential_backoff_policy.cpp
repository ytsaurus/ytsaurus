#include "pod_exponential_backoff_policy.h"

#include "config.h"

#include <yp/server/lib/cluster/cluster.h>
#include <yp/server/lib/cluster/pod.h>

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TPodExponentialBackoffPolicy::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(TPodExponentialBackoffPolicyConfigPtr config)
        : Config_(std::move(config))
    { }

    TDuration GetNextBackoffDuration(const NCluster::TPod* pod)
    {
        VERIFY_THREAD_AFFINITY(SchedulerLoopThread);

        TDuration backoffDuration = Config_->Start;
        auto key = std::make_pair(pod->GetId(), pod->Uuid());
        if (auto it = LastBackoffDurations_.find(key); it != LastBackoffDurations_.end()) {
            backoffDuration = std::min(it->second * Config_->Base, Config_->Max);
        }
        LastBackoffDurations_[key] = backoffDuration;
        return backoffDuration;
    }

    void ReconcileState(const NYP::NServer::NCluster::TClusterPtr& cluster)
    {
        VERIFY_THREAD_AFFINITY(SchedulerLoopThread);

        THashMap<std::pair<TObjectId, TObjectId>, TDuration> newLastBackoffDurations;

        for (const auto& [key, value] : LastBackoffDurations_) {
            if (!NeedErase(cluster, key.first, key.second)) {
                newLastBackoffDurations[key] = value;
            }
        }
        LastBackoffDurations_.swap(newLastBackoffDurations);
    }

private:
    const TPodExponentialBackoffPolicyConfigPtr Config_;

    THashMap<std::pair<TObjectId, TObjectId>, TDuration> LastBackoffDurations_;

    DECLARE_THREAD_AFFINITY_SLOT(SchedulerLoopThread);

    static bool NeedErase(
        const NCluster::TClusterPtr& cluster,
        const TObjectId& podId,
        const TObjectId& podUuid)
    {
        auto* pod = cluster->FindPod(podId);
        return !pod || podUuid != pod->Uuid() || pod->GetNode();
    }
};

TPodExponentialBackoffPolicy::TPodExponentialBackoffPolicy(TPodExponentialBackoffPolicyConfigPtr config)
    : Impl_(New<TPodExponentialBackoffPolicy::TImpl>(std::move(config)))
{ }

TDuration TPodExponentialBackoffPolicy::GetNextBackoffDuration(const NCluster::TPod* pod)
{
    return Impl_->GetNextBackoffDuration(pod);
}

void TPodExponentialBackoffPolicy::ReconcileState(const NYP::NServer::NCluster::TClusterPtr& cluster)
{
    Impl_->ReconcileState(cluster);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
