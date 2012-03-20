#include "stdafx.h"
#include "bootstrap.h"
#include "config.h"
#include "job_manager.h"
#include "supervisor_service.h"

#include <ytlib/cell_node/bootstrap.h>

namespace NYT {
namespace NExecAgent {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
    TJobManagerConfigPtr config,
    NCellNode::TBootstrap* nodeBootstrap)
    : Config(config)
    , NodeBootstrap(nodeBootstrap)
{ }

TBootstrap::~TBootstrap()
{ }

void TBootstrap::Init()
{
    JobManager = New<TJobManager>(Config, this);

    auto supervisorService = New<TSupervisorService>(this);
    NodeBootstrap->GetRpcServer()->RegisterService(supervisorService);
}

TJobManagerConfigPtr TBootstrap::GetConfig() const
{
    return Config;
}

IInvoker::TPtr TBootstrap::GetControlInvoker() const
{
    return NodeBootstrap->GetControlInvoker();
}

IChannel::TPtr TBootstrap::GetLeaderChannel() const
{
    return NodeBootstrap->GetLeaderChannel();
}

Stroka TBootstrap::GetPeerAddress() const
{
    return NodeBootstrap->GetPeerAddress();
}

TJobManagerPtr TBootstrap::GetJobManager() const
{
    return JobManager;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
