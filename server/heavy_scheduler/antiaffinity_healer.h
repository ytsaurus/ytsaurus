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
        THeavyScheduler* heavyScheduler,
        TAntiaffinityHealerConfigPtr config);

    void Run(const NCluster::TClusterPtr& cluster);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TAntiaffinityHealer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
