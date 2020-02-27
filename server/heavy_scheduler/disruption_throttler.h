#pragma once

#include "public.h"

#include <yp/server/lib/cluster/pod.h>

namespace NYP::NServer::NHeavyScheduler {

////////////////////////////////////////////////////////////////////////////////

class TDisruptionThrottler
    : public TRefCounted
{
public:
    TDisruptionThrottler(
        THeavyScheduler* heavyScheduler,
        TDisruptionThrottlerConfigPtr config);

    void ReconcileState(const NCluster::TClusterPtr& cluster);

    void RegisterPodEviction(NCluster::TPod* pod);

    bool ThrottleEviction(NCluster::TPod* pod) const;

    int EvictionCount() const;

    bool IsBeingEvicted(const NCluster::TObjectId& podId) const;

    bool GetValidatePodDisruptionBudget() const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TDisruptionThrottler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
