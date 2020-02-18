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
        THeavyScheduler* heavyScheduler,
        TSwapDefragmentatorConfigPtr config);

    void CreateTasks(const NCluster::TClusterPtr& cluster);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TSwapDefragmentator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
