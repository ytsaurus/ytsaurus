#pragma once

#include "public.h"
#include "config.h"

#include <ytlib/rpc/public.h>

#include <ytlib/chunk_client/node_directory.h>

#include <server/cell_node/public.h>

#include <server/chunk_holder/public.h>

#include <server/job_proxy/public.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(
        TExecAgentConfigPtr config,
        NCellNode::TBootstrap* nodeBootstrap);
    ~TBootstrap();

    void Initialize();

    TExecAgentConfigPtr GetConfig() const;
    IInvokerPtr GetControlInvoker() const;
    NRpc::IChannelPtr GetMasterChannel() const;
    NRpc::IChannelPtr GetSchedulerChannel() const;
    const NChunkClient::TNodeDescriptor& GetLocalDescriptor() const;
    TJobManagerPtr GetJobManager() const;
    TEnvironmentManagerPtr GetEnvironmentManager() const;
    NChunkHolder::TChunkCachePtr GetChunkCache() const;
    NJobProxy::TJobProxyConfigPtr GetJobProxyConfig() const;
    NCellNode::TNodeMemoryTracker& GetMemoryUsageTracker();
    bool IsJobControlEnabled() const;

private:
    TExecAgentConfigPtr Config;
    NCellNode::TBootstrap* NodeBootstrap;

    bool JobControlEnabled;

    TJobManagerPtr JobManager;
    TEnvironmentManagerPtr EnvironmentManager;
    TSchedulerConnectorPtr SchedulerConnector;
    NJobProxy::TJobProxyConfigPtr JobProxyConfig;
    TAddressResolverConfigPtr AddressResolver;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
