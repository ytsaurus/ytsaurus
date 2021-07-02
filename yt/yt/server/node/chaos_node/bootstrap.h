#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
    : public virtual NClusterNode::IBootstrapBase
{
    virtual ~IBootstrap() = default;

    virtual void Initialize() = 0;
    virtual void Run() = 0;

    // Invokers.
    virtual const IInvokerPtr& GetTransactionTrackerInvoker() const = 0;

    // Chaos cells stuff.
    virtual const NCellarAgent::ICellarManagerPtr& GetCellarManager() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(NClusterNode::IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
