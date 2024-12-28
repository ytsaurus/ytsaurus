#pragma once

#include "public.h"

#include <yt/yt/server/lib/misc/bootstrap.h>

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NReplicatedTableTracker {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
    : public NServer::IDaemonBootstrap
{
    virtual const TReplicatedTableTrackerBootstrapConfigPtr& GetServerConfig() const = 0;
    virtual const TDynamicConfigManagerPtr& GetDynamicConfigManager() const = 0;
    virtual const NApi::NNative::IConnectionPtr& GetClusterConnection() const = 0;
    virtual const IInvokerPtr& GetRttHostInvoker() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IBootstrap)

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateReplicatedTableTrackerBootstrap(
    TReplicatedTableTrackerBootstrapConfigPtr config,
    NYTree::INodePtr configNode,
    NFusion::IServiceLocatorPtr serviceLocator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NReplicatedTableTracker
