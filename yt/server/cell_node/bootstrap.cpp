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

#include <ytlib/meta_state/master_channel.h>

#include <ytlib/meta_state/config.h>

#include <ytlib/orchid/orchid_service.h>

#include <ytlib/monitoring/monitoring_manager.h>
#include <ytlib/monitoring/ytree_integration.h>
#include <ytlib/monitoring/http_server.h>
#include <ytlib/monitoring/http_integration.h>

#include <ytlib/ytree/ephemeral_node_factory.h>
#include <ytlib/ytree/virtual.h>
#include <ytlib/ytree/yson_file_service.h>
#include <ytlib/ytree/ypath_client.h>

#include <ytlib/profiling/profiling_manager.h>

#include <ytlib/scheduler/scheduler_channel.h>

#include <server/chunk_holder/bootstrap.h>
#include <server/chunk_holder/config.h>
#include <server/chunk_holder/ytree_integration.h>
#include <server/chunk_holder/chunk_cache.h>

#include <server/exec_agent/bootstrap.h>
#include <server/exec_agent/config.h>

namespace NYT {
namespace NCellNode {

using namespace NBus;
using namespace NChunkServer;
using namespace NElection;
using namespace NMonitoring;
using namespace NOrchid;
using namespace NProfiling;
using namespace NRpc;
using namespace NScheduler;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Bootstrap");

const i64 FootprintMemorySize = 1024L * 1024 * 1024;
////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
    const Stroka& configFileName,
    TCellNodeConfigPtr config)
    : ConfigFileName(configFileName)
    , Config(config)
    , MemoryUsageTracker(Config->TotalMemorySize, "/cell_node")
{ }

TBootstrap::~TBootstrap()
{ }

void TBootstrap::Run()
{
    srand(time(NULL));

    IncarnationId = TIncarnationId::Create();
    PeerAddress = BuildServiceAddress(GetLocalHostName(), Config->RpcPort);

    LOG_INFO("Starting node (IncarnationId: %s, PeerAddress: %s, MasterAddresses: [%s])",
        ~IncarnationId.ToString(),
        ~PeerAddress,
        ~JoinToString(Config->Masters->Addresses));

    auto result = MemoryUsageTracker.TryAcquire(
        EMemoryConsumer::Footprint,
        FootprintMemorySize);
    if (!result.IsOK()) {
        auto error = TError("Error allocating footprint memory") << result;
        // TODO(psushin): no need to create core here.
        LOG_FATAL(error);
    }

    MasterChannel = CreateLeaderChannel(Config->Masters);
    
    SchedulerChannel = CreateSchedulerChannel(
        Config->ExecAgent->SchedulerConnector->RpcTimeout,
        MasterChannel);

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

TNodeMemoryTracker& TBootstrap::GetMemoryUsageTracker()
{
    return MemoryUsageTracker;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
