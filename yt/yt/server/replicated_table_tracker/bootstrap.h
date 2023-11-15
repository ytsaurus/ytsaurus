#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NReplicatedTableTracker {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
{
    virtual ~IBootstrap() = default;

    virtual void Run() = 0;

    virtual const TReplicatedTableTrackerServerConfigPtr& GetServerConfig() const = 0;

    virtual const TDynamicConfigManagerPtr& GetDynamicConfigManager() const = 0;

    virtual const NApi::NNative::IConnectionPtr& GetClusterConnection() const = 0;

    virtual const IInvokerPtr& GetRttHostInvoker() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(
    TReplicatedTableTrackerServerConfigPtr config,
    NYTree::INodePtr configNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NReplicatedTableTracker
