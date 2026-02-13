#pragma once

#include "public.h"

#include <yt/yt/server/node/cellar_node/public.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IMediumThrottlerManager
    : public TRefCounted
{
    virtual NConcurrency::IReconfigurableThroughputThrottlerPtr GetOrCreateMediumWriteThrottler(
        const std::string& mediumName,
        ETabletDistributedThrottlerKind kind) = 0;

    virtual NConcurrency::IReconfigurableThroughputThrottlerPtr GetOrCreateMediumReadThrottler(
        const std::string& mediumName,
        ETabletDistributedThrottlerKind kind) = 0;
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
    NCellarNode::TBundleDynamicConfigManagerPtr dynamicConfigManager,
    IDistributedThrottlerManagerPtr distributedThrottlerManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
