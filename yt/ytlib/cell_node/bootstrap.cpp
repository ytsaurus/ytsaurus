#include "stdafx.h"
#include "bootstrap.h"
#include "config.h"

#include <ytlib/misc/address.h>
#include <ytlib/misc/ref_counted_tracker.h>

#include <ytlib/actions/action_queue.h>

#include <ytlib/bus/tcp_server.h>
#include <ytlib/bus/config.h>

#include <ytlib/rpc/channel.h>
#include <ytlib/rpc/server.h>

#include <ytlib/election/leader_channel.h>

#include <ytlib/orchid/orchid_service.h>

#include <ytlib/monitoring/monitoring_manager.h>
#include <ytlib/monitoring/ytree_integration.h>
#include <ytlib/monitoring/http_server.h>
#include <ytlib/monitoring/http_integration.h>

#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/virtual.h>
#include <ytlib/ytree/yson_file_service.h>
#include <ytlib/ytree/ypath_client.h>

#include <ytlib/profiling/profiling_manager.h>

#include <ytlib/chunk_holder/bootstrap.h>
#include <ytlib/chunk_holder/config.h>
#include <ytlib/chunk_holder/ytree_integration.h>
#include <ytlib/chunk_holder/chunk_cache.h>

#include <ytlib/scheduler/scheduler_channel.h>

#include <ytlib/exec_agent/bootstrap.h>
#include <ytlib/exec_agent/config.h>

namespace NYT {
namespace NCellNode {

using namespace NBus;
using namespace NRpc;
using namespace NYTree;
using namespace NMonitoring;
using namespace NOrchid;
using namespace NChunkServer;
using namespace NProfiling;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Bootstrap");

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
    const Stroka& configFileName,
    TCellNodeConfigPtr config)
    : ConfigFileName(configFileName)
    , Config(config)
{ }

TBootstrap::~TBootstrap()
{ }

void TBootstrap::Run()
{
    IncarnationId = TIncarnationId::Create();
    PeerAddress = BuildServiceAddress(GetLocalHostName(), Config->RpcPort);

    LOG_INFO("Starting node (IncarnationId: %s, PeerAddress: %s, MasterAddresses: [%s])",
        ~IncarnationId.ToString(),
        ~PeerAddress,
        ~JoinToString(Config->Masters->Addresses));

    MasterChannel = CreateLeaderChannel(Config->Masters);
    
    // TODO(babenko): for now we use the same timeout both for masters and scheduler
    SchedulerChannel = CreateSchedulerChannel(Config->Masters->RpcTimeout, MasterChannel);

    ControlQueue = New<TActionQueue>("Control");

    BusServer = CreateTcpBusServer(New<TTcpBusServerConfig>(Config->RpcPort));

    RpcServer = CreateRpcServer(BusServer);

    auto monitoringManager = New<TMonitoringManager>();
    monitoringManager->Register(
        "/ref_counted",
        BIND(&TRefCountedTracker::GetMonitoringInfo, TRefCountedTracker::Get()));
    monitoringManager->Start();

    OrchidRoot = GetEphemeralNodeFactory()->CreateMap();
    SetNodeByYPath(
        OrchidRoot,
        "/monitoring",
        CreateVirtualNode(CreateMonitoringProducer(monitoringManager)));
    SetNodeByYPath(
        OrchidRoot,
        "/profiling",
        CreateVirtualNode(
            TProfilingManager::Get()->GetRoot()
            ->Via(TProfilingManager::Get()->GetInvoker())));
    SetNodeByYPath(
        OrchidRoot,
        "/config",
        CreateVirtualNode(CreateYsonFileProducer(ConfigFileName)));

    auto orchidService = New<TOrchidService>(
        OrchidRoot,
        GetControlInvoker());
    RpcServer->RegisterService(orchidService);

    ::THolder<NHttp::TServer> httpServer(new NHttp::TServer(Config->MonitoringPort));
    httpServer->Register(
        "/orchid",
        NMonitoring::GetYPathHttpHandler(OrchidRoot->Via(GetControlInvoker())));

    ChunkHolderBootstrap.Reset(new NChunkHolder::TBootstrap(Config->DataNode, this));
    ChunkHolderBootstrap->Init();

    ExecAgentBootstrap.Reset(new NExecAgent::TBootstrap(Config->ExecAgent, this));
    ExecAgentBootstrap->Init();

    LOG_INFO("Listening for HTTP requests on port %d", Config->MonitoringPort);
    httpServer->Start();

    LOG_INFO("Listening for RPC requests on port %d", Config->RpcPort);
    RpcServer->Start();

    Sleep(TDuration::Max());
}

TCellNodeConfigPtr TBootstrap::GetConfig() const
{
    return Config;
}

TIncarnationId TBootstrap::GetIncarnationId() const
{
    return IncarnationId;
}

IInvokerPtr TBootstrap::GetControlInvoker() const
{
    return ControlQueue->GetInvoker();
}

IServerPtr TBootstrap::GetRpcServer() const
{
    return RpcServer;
}

IChannelPtr TBootstrap::GetMasterChannel() const
{
    return MasterChannel;
}

IChannelPtr TBootstrap::GetSchedulerChannel() const
{
    return SchedulerChannel;
}

Stroka TBootstrap::GetPeerAddress() const
{
    return PeerAddress;
}

IMapNodePtr TBootstrap::GetOrchidRoot() const
{
    return OrchidRoot;
}

NChunkHolder::TBootstrap* TBootstrap::GetChunkHolderBootstrap() const
{
    return ChunkHolderBootstrap.Get();
}

NExecAgent::TBootstrap* TBootstrap::GetExecAgentBootstrap() const
{
    return ExecAgentBootstrap.Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
