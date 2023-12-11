#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct ITabletBalancer
    : public TRefCounted
{
    virtual void Start() = 0;
    virtual void Stop() = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() = 0;

    virtual void OnDynamicConfigChanged(
        const TTabletBalancerDynamicConfigPtr& oldConfig,
        const TTabletBalancerDynamicConfigPtr& newConfig) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITabletBalancer)

////////////////////////////////////////////////////////////////////////////////

ITabletBalancerPtr CreateTabletBalancer(
    IBootstrap* bootstrap,
    TStandaloneTabletBalancerConfigPtr config,
    IInvokerPtr controlInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
