#pragma once

#include "public.h"

#include "task.h"
#include "disruption_throttler.h"

namespace NYP::NServer::NHeavyScheduler {

////////////////////////////////////////////////////////////////////////////////

class TSwapDefragmentator
    : public TRefCounted
{
public:
    TSwapDefragmentator(
        TSwapDefragmentatorConfigPtr config,
        NClient::NApi::NNative::IClientPtr client,
        TObjectId nodeSegment,
        bool verbose);

    std::vector<ITaskPtr> CreateTasks(
        const NCluster::TClusterPtr& cluster,
        const TDisruptionThrottlerPtr& disruptionThrottler,
        const THashSet<NCluster::TObjectId>& ignorePodIds,
        int maxTaskCount,
        int currentTotalTaskCount);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TSwapDefragmentator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
