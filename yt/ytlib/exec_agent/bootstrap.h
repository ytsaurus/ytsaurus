#pragma once

#include "public.h"
#include "config.h"

#include <ytlib/cell_node/public.h>
#include <ytlib/chunk_holder/public.h>
#include <ytlib/job_proxy/public.h>
// TODO(babenko): replace with public.h
#include <ytlib/rpc/channel.h>

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
    IInvoker::TPtr GetControlInvoker() const;
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
