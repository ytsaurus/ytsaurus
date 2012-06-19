#pragma once

#include "public.h"
#include "config.h"

#include <ytlib/cell_node/public.h>
#include <ytlib/chunk_holder/public.h>
#include <ytlib/job_proxy/public.h>
#include <ytlib/rpc/public.h>

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
    
    void Init();

    TExecAgentConfigPtr GetConfig() const;
    IInvokerPtr GetControlInvoker() const;
    NRpc::IChannelPtr GetMasterChannel() const;
    NRpc::IChannelPtr GetSchedulerChannel() const;
    Stroka GetPeerAddress() const;
    TJobManagerPtr GetJobManager() const;
    TEnvironmentManagerPtr GetEnvironmentManager() const;
    NChunkHolder::TChunkCachePtr GetChunkCache() const;
    NJobProxy::TJobProxyConfigPtr GetJobProxyConfig() const;

private:
    TExecAgentConfigPtr Config;
    NCellNode::TBootstrap* NodeBootstrap;

    TJobManagerPtr JobManager;
    TEnvironmentManagerPtr EnvironmentManager;
    TSchedulerConnectorPtr SchedulerConnector;
    NJobProxy::TJobProxyConfigPtr JobProxyConfig;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
