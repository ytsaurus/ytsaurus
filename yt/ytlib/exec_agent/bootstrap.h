#pragma once

#include "public.h"

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

    TExecAgentConfigPtr GetConfig() const;
    NRpc::IChannel::TPtr GetLeaderChannel() const;
    TJobManagerPtr GetJobManager() const;

private:
	TExecAgentConfigPtr Config;
	NCellNode::TBootstrap* NodeBootstrap;
    
    TJobManagerPtr JobManager;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
