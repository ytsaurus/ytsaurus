#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_balancer/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct IClusterStateProvider
    : public TRefCounted
{
    virtual void Start() = 0;
    virtual void Stop() = 0;

    virtual void Reconfigure(TClusterStateProviderConfigPtr config) = 0;

    virtual TFuture<NYTree::IListNodePtr> GetBundles() = 0;
    virtual TFuture<NYTree::IListNodePtr> GetNodes() = 0;
    virtual TFuture<THashMap<std::string, std::vector<std::string>>> GetUnhealthyBundles() = 0;
    virtual TFuture<THashSet<std::string>> GetBannedReplicasFromMetaCluster() = 0;
};

DEFINE_REFCOUNTED_TYPE(IClusterStateProvider)

////////////////////////////////////////////////////////////////////////////////

IClusterStateProviderPtr CreateClusterStateProvider(
    IBootstrap* bootstrap,
    TClusterStateProviderConfigPtr config,
    IInvokerPtr controlInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
