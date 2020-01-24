#pragma once

#include "public.h"

#include "task.h"

namespace NYP::NServer::NHeavyScheduler {

////////////////////////////////////////////////////////////////////////////////

class TAntiaffinityHealer
    : public TRefCounted
{
public:
    TAntiaffinityHealer(
        TAntiaffinityHealerConfigPtr config,
        NClient::NApi::NNative::IClientPtr client,
        bool verbose);

    std::vector<ITaskPtr> CreateTasks(
        const NCluster::TClusterPtr& cluster,
        const TDisruptionThrottlerPtr& disruptionThrottler,
        const THashSet<TObjectId>& ignorePodIds,
        int maxTaskCount,
        int currentTotalTaskCount);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TAntiaffinityHealer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
