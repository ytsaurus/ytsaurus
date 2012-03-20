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
    	TJobManagerConfigPtr config,
	    NCellNode::TBootstrap* nodeBootstrap);
    ~TBootstrap();
    
    void Init();

    TJobManagerConfigPtr GetConfig() const;
    IInvoker::TPtr GetControlInvoker() const;
    NRpc::IChannel::TPtr GetLeaderChannel() const;
    Stroka GetPeerAddress() const;
    TJobManagerPtr GetJobManager() const;
    TEnvironmentManagerPtr GetEnvironmentManager() const;

private:
	TJobManagerConfigPtr Config;
	NCellNode::TBootstrap* NodeBootstrap;
    
    TJobManagerPtr JobManager;
    TEnvironmentManagerPtr EnvironmentManager;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
