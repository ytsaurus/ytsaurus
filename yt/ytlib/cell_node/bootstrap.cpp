#include "stdafx.h"
#include "bootstrap.h"
#include "config.h"

#include <ytlib/misc/ref_counted_tracker.h>

#include <ytlib/actions/action_queue.h>

#include <ytlib/bus/nl_server.h>

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
    PeerAddress = Sprintf("%s:%d", ~HostName(), Config->RpcPort);

    LOG_INFO("Starting node (IncarnationId: %s, PeerAddress: %s, MasterAddresses: [%s])",
        ~IncarnationId.ToString(),
        ~PeerAddress,
        ~JoinToString(Config->Masters->Addresses));

    LeaderChannel = CreateLeaderChannel(~Config->Masters);

    auto controlQueue = New<TActionQueue>("Control");
    ControlInvoker = controlQueue->GetInvoker();

    BusServer = CreateNLBusServer(~New<TNLBusServerConfig>(Config->RpcPort));

    RpcServer = CreateRpcServer(~BusServer);

    auto monitoringManager = New<TMonitoringManager>();
    monitoringManager->Register(
        "ref_counted",
        FromMethod(&TRefCountedTracker::GetMonitoringInfo, TRefCountedTracker::Get()));
    monitoringManager->Register(
        "bus_server",
        FromMethod(&IBusServer::GetMonitoringInfo, BusServer));
    monitoringManager->Start();

    OrchidRoot = GetEphemeralNodeFactory()->CreateMap();
    SyncYPathSetNode(
        ~OrchidRoot,
        "monitoring",
        ~NYTree::CreateVirtualNode(~CreateMonitoringProducer(~monitoringManager)));
    SyncYPathSetNode(
        ~OrchidRoot,
        "profiling",
        ~CreateVirtualNode(
            ~TProfilingManager::Get()->GetRoot()
            ->Via(TProfilingManager::Get()->GetInvoker())));
    SyncYPathSetNode(
        ~OrchidRoot,
        "config",
        ~NYTree::CreateVirtualNode(~NYTree::CreateYsonFileProducer(ConfigFileName)));

    auto orchidService = New<TOrchidService>(
        ~OrchidRoot,
        controlQueue->GetInvoker());
    RpcServer->RegisterService(~orchidService);

    ::THolder<NHttp::TServer> httpServer(new NHttp::TServer(Config->MonitoringPort));
    httpServer->Register(
        "/orchid",
        ~NMonitoring::GetYPathHttpHandler(~OrchidRoot->Via(controlQueue->GetInvoker())));

    NChunkHolder::TBootstrap dataNodeBootstrap(Config->Data, this);
    dataNodeBootstrap.Init();

    NExecAgent::TBootstrap execNodeBootstrap(Config->Exec, this);
    execNodeBootstrap.Init();

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

IInvoker::TPtr TBootstrap::GetControlInvoker() const
{
    return ControlInvoker;
}

IBusServer::TPtr TBootstrap::GetBusServer() const
{
    return BusServer;
}

IServer::TPtr TBootstrap::GetRpcServer() const
{
    return RpcServer;
}

IChannel::TPtr TBootstrap::GetLeaderChannel() const
{
    return LeaderChannel;
}

Stroka TBootstrap::GetPeerAddress() const
{
    return PeerAddress;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
