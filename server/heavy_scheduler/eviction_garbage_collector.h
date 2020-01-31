#pragma once

#include "public.h"

namespace NYP::NServer::NHeavyScheduler {

////////////////////////////////////////////////////////////////////////////////

class TEvictionGarbageCollector
    : public TRefCounted
{
public:
    TEvictionGarbageCollector(
        TEvictionGarbageCollectorConfigPtr config,
        NClient::NApi::NNative::IClientPtr client);

    void Run(const NCluster::TClusterPtr& cluster);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TEvictionGarbageCollector);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
