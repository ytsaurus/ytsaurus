#include "stdafx.h"
#include "bootstrap.h"
#include "config.h"
#include "private.h"
#include "job.h"
#include "job_manager.h"
#include "supervisor_service.h"
#include "environment.h"
#include "environment_manager.h"
#include "unsafe_environment.h"
#include "scheduler_connector.h"
#include "slot.h"

#include <ytlib/cell_node/bootstrap.h>
#include <ytlib/cell_node/config.h>
#include <ytlib/job_proxy/config.h>
#include <ytlib/chunk_holder/bootstrap.h>
#include <ytlib/chunk_holder/config.h>
#include <ytlib/chunk_holder/chunk_cache.h>
#include <ytlib/rpc/server.h>

namespace NYT {
namespace NExecAgent {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
    TExecAgentConfigPtr config,
    NCellNode::TBootstrap* nodeBootstrap)
    : Config(config)
    , NodeBootstrap(nodeBootstrap)
{
    YASSERT(config);
    YASSERT(nodeBootstrap);
}

TBootstrap::~TBootstrap()
{ }

void TBootstrap::Init()
{
    JobProxyConfig = New<NJobProxy::TJobProxyConfig>();
    JobProxyConfig->RpcTimeout = Config->SupervisorTimeout;
    JobProxyConfig->Logging = Config->JobProxyLogging;
    JobProxyConfig->SandboxName = SandboxName;
    JobProxyConfig->Masters = NodeBootstrap->GetConfig()->Masters;
    JobProxyConfig->ExecAgentAddress = NodeBootstrap->GetPeerAddress();

    JobManager = New<TJobManager>(Config->JobManager, this);

    auto supervisorService = New<TSupervisorService>(this);
    NodeBootstrap->GetRpcServer()->RegisterService(supervisorService);

    EnvironmentManager = New<TEnvironmentManager>(Config->EnvironmentManager);
    EnvironmentManager->Register("unsafe",  CreateUnsafeEnvironmentBuilder());

    SchedulerConnector = New<TSchedulerConnector>(Config->SchedulerConnector, this);
    SchedulerConnector->Start();
}

TExecAgentConfigPtr TBootstrap::GetConfig() const
{
    return Config;
}

IInvoker::TPtr TBootstrap::GetControlInvoker(NCellNode::EControlThreadQueue queueIndex) const
{
    return NodeBootstrap->GetControlInvoker(queueIndex);
}

IChannelPtr TBootstrap::GetMasterChannel() const
{
    return NodeBootstrap->GetMasterChannel();
}

IChannelPtr TBootstrap::GetSchedulerChannel() const
{
    return NodeBootstrap->GetSchedulerChannel();
}

Stroka TBootstrap::GetPeerAddress() const
{
    return NodeBootstrap->GetPeerAddress();
}

TJobManagerPtr TBootstrap::GetJobManager() const
{
    return JobManager;
}

TEnvironmentManagerPtr TBootstrap::GetEnvironmentManager() const
{
    return EnvironmentManager;
}

NChunkHolder::TChunkCachePtr TBootstrap::GetChunkCache() const
{
    return NodeBootstrap->GetChunkHolderBootstrap()->GetChunkCache();
}

NJobProxy::TJobProxyConfigPtr TBootstrap::GetJobProxyConfig() const
{
    return JobProxyConfig;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
