#pragma once

#include "public.h"

namespace NYP::NServer::NHeavyScheduler {

////////////////////////////////////////////////////////////////////////////////

class TEvictionGarbageCollector
    : public TRefCounted
{
public:
    explicit TEvictionGarbageCollector(
        THeavyScheduler* heavyScheduler,
        TEvictionGarbageCollectorConfigPtr config);
    ~TEvictionGarbageCollector();

    void Run(const NCluster::TClusterPtr& cluster);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TEvictionGarbageCollector);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
