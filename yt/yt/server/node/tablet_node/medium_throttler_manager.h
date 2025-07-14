#pragma once

#include "public.h"

#include <yt/yt/server/node/cellar_node/public.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NTabletNode {

using namespace NConcurrency;
using namespace NCellarNode;

////////////////////////////////////////////////////////////////////////////////

struct IMediumThrottlerManager
    : public TRefCounted
{
    virtual IReconfigurableThroughputThrottlerPtr GetMediumWriteThrottler(const std::string& mediumName) = 0;

    virtual IReconfigurableThroughputThrottlerPtr GetMediumReadThrottler(const std::string& mediumName) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMediumThrottlerManager)

////////////////////////////////////////////////////////////////////////////////

struct IMediumThrottlerManagerFactory
    : public TRefCounted
{
    virtual IMediumThrottlerManagerPtr GetOrCreateMediumThrottlerManager(const std::string& bundleName) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMediumThrottlerManagerFactory)

////////////////////////////////////////////////////////////////////////////////

IMediumThrottlerManagerFactoryPtr CreateMediumThrottlerManagerFactory(
    TBundleDynamicConfigManagerPtr dynamicConfigManager,
    IDistributedThrottlerManagerPtr distributedThrottlerManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode::NYT
