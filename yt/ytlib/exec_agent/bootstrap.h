#pragma once

#include "public.h"
#include "config.h"

#include <ytlib/cell_node/public.h>
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
    NRpc::IChannel::TPtr GetMasterChannel() const;
    NRpc::IChannel::TPtr GetSchedulerChannel() const;
    Stroka GetPeerAddress() const;
    TJobManagerPtr GetJobManager() const;
    TEnvironmentManagerPtr GetEnvironmentManager() const;

private:
	TExecAgentConfigPtr Config;
	NCellNode::TBootstrap* NodeBootstrap;
    
    TJobManagerPtr JobManager;
    TEnvironmentManagerPtr EnvironmentManager;
    TSchedulerConnectorPtr SchedulerConnector;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
