#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/lib/security_server/public.h>

namespace NYT::NCellarNode {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrapDryRunBase
{
    virtual void LoadSnapshot(
        const TString& fileName,
        NHydra::NProto::TSnapshotMeta meta = {},
        bool dump = false) = 0;
    virtual void ReplayChangelogs(std::vector<TString> changelogFileNames) = 0;
    virtual void BuildSnapshot() = 0;
    virtual void FinishDryRun() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
    : public virtual NClusterNode::IBootstrapBase
    , public virtual IBootstrapDryRunBase
{
    virtual void Initialize() = 0;
    virtual void Run() = 0;

    virtual const NTransactionSupervisor::ITransactionLeaseTrackerThreadPoolPtr& GetTransactionLeaseTrackerThreadPool() const = 0;

    virtual const NSecurityServer::IResourceLimitsManagerPtr& GetResourceLimitsManager() const = 0;

    virtual const NCellarAgent::ICellarManagerPtr& GetCellarManager() const = 0;

    virtual const NCellarNode::IMasterConnectorPtr& GetMasterConnector() const = 0;

    virtual void ScheduleCellarHeartbeat() const = 0;

    virtual const NConcurrency::IThroughputThrottlerPtr& GetChangelogOutThrottler() const = 0;
    virtual const NConcurrency::IThroughputThrottlerPtr& GetSnapshotOutThrottler() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(NClusterNode::IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarNode
