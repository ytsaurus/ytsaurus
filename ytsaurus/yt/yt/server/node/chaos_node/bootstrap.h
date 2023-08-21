#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/lib/tablet_server/public.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

using TReplicatedTableTrackerConfigUpdateCallback = TCallback<void(
    const NTabletServer::TDynamicReplicatedTableTrackerConfigPtr& /*oldConfig*/,
    const NTabletServer::TDynamicReplicatedTableTrackerConfigPtr& /*newConfig*/)>;

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
    virtual const IShortcutSnapshotStorePtr& GetShortcutSnapshotStore() const = 0;
    virtual const IInvokerPtr& GetSnapshotStoreReadPoolInvoker() const = 0;

    // Replicated table tracker stuff.
    virtual void SubscribeReplicatedTableTrackerConfigChanged(TReplicatedTableTrackerConfigUpdateCallback callback) const = 0;
    virtual NTabletServer::TDynamicReplicatedTableTrackerConfigPtr GetReplicatedTableTrackerConfig() const = 0;

    // Master connection stuff.
    virtual const NApi::NNative::IConnectionPtr& GetClusterConnection() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(NClusterNode::IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
