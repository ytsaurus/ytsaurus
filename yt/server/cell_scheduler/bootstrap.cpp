#include "stdafx.h"
#include "bootstrap.h"
#include "config.h"

#include <core/misc/address.h>
#include <core/misc/ref_counted_tracker.h>

#include <core/concurrency/action_queue.h>
#include <core/concurrency/throughput_throttler.h>

#include <core/bus/server.h>
#include <core/bus/tcp_server.h>
#include <core/bus/config.h>

#include <core/rpc/server.h>
#include <core/rpc/retrying_channel.h>

#include <ytlib/meta_state/master_channel.h>

#include <ytlib/meta_state/config.h>

#include <ytlib/orchid/orchid_service.h>

#include <ytlib/monitoring/monitoring_manager.h>
#include <ytlib/monitoring/http_server.h>
#include <ytlib/monitoring/http_integration.h>

#include <core/ytree/virtual.h>
#include <core/ytree/ypath_client.h>
#include <core/ytree/yson_file_service.h>

#include <core/profiling/profiling_manager.h>

#include <ytlib/scheduler/config.h>

#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/cell_directory/cell_directory.h>

#include <server/misc/build_attributes.h>

#include <server/job_proxy/config.h>

#include <server/scheduler/scheduler.h>
#include <server/scheduler/scheduler_service.h>
#include <server/scheduler/job_tracker_service.h>
#include <server/scheduler/config.h>


namespace NYT {
namespace NCellScheduler {

using namespace NBus;
using namespace NElection;
using namespace NMonitoring;
using namespace NObjectClient;
using namespace NOrchid;
using namespace NProfiling;
using namespace NRpc;
using namespace NScheduler;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("SchedulerBootstrap");

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
    const Stroka& configFileName,
    TCellSchedulerConfigPtr config)
    : ConfigFileName(configFileName)
    , Config(config)
{ }

TBootstrap::~TBootstrap()
{ }

void TBootstrap::Run()
{
    LocalAddress = BuildServiceAddress(
        TAddressResolver::Get()->GetLocalHostName(),
        Config->RpcPort);

    LOG_INFO("Starting scheduler (LocalAddress: %s, MasterAddresses: [%s])",
        ~LocalAddress,
        ~JoinToString(Config->Masters->Addresses));

    MasterChannel = CreateLeaderChannel(Config->Masters);

    ControlQueue = New<TFairShareActionQueue>("Control", EControlQueue::GetDomainNames());

    BusServer = CreateTcpBusServer(New<TTcpBusServerConfig>(Config->RpcPort));

    auto rpcServer = CreateRpcServer(BusServer);

    TransactionManager = New<TTransactionManager>(
        Config->TransactionManager,
        MasterChannel);

    ChunkLocationThrottler = CreateLimitedThrottler(Config->Scheduler->ChunkLocationThrottler);

    CellDirectory = New<NCellDirectory::TCellDirectory>(MasterChannel);

    Scheduler = New<TScheduler>(Config->Scheduler, this);

    auto monitoringManager = New<TMonitoringManager>();
    monitoringManager->Register(
        "/ref_counted",
        BIND(&TRefCountedTracker::GetMonitoringInfo, TRefCountedTracker::Get()));
    monitoringManager->Start();

    auto orchidFactory = NYTree::GetEphemeralNodeFactory();
    auto orchidRoot = orchidFactory->CreateMap();
    SetNodeByYPath(
        orchidRoot,
        "/monitoring",
        CreateVirtualNode(monitoringManager->GetService()));
    SetNodeByYPath(
        orchidRoot,
        "/profiling",
        CreateVirtualNode(TProfilingManager::Get()->GetService()));
    SetNodeByYPath(
        orchidRoot,
        "/config",
        CreateVirtualNode(NYTree::CreateYsonFileService(ConfigFileName)));
    SetNodeByYPath(
        orchidRoot,
        "/scheduler",
        CreateVirtualNode(Scheduler
            ->GetOrchidService()
            ->Via(GetControlInvoker())
            ->Cached(Config->OrchidCacheExpirationPeriod)));
    
    SetBuildAttributes(orchidRoot, "scheduler");

    rpcServer->RegisterService(CreateOrchidService(
        orchidRoot,
        GetControlInvoker()));

    NHttp::TServer httpServer(Config->MonitoringPort);
    httpServer.Register(
        "/orchid",
        NMonitoring::GetYPathHttpHandler(orchidRoot));

    rpcServer->RegisterService(CreateSchedulerService(this));
    rpcServer->RegisterService(CreateJobTrackerService(this));

    LOG_INFO("Listening for HTTP requests on port %d", Config->MonitoringPort);
    httpServer.Start();

    LOG_INFO("Listening for RPC requests on port %d", Config->RpcPort);
    rpcServer->Configure(Config->RpcServer);
    rpcServer->Start();

    Scheduler->Initialize();

    Sleep(TDuration::Max());
}

TCellSchedulerConfigPtr TBootstrap::GetConfig() const
{
    return Config;
}

IChannelPtr TBootstrap::GetMasterChannel() const
{
    return MasterChannel;
}

const Stroka& TBootstrap::GetLocalAddress() const
{
    return LocalAddress;
}

IInvokerPtr TBootstrap::GetControlInvoker(EControlQueue queue) const
{
    return ControlQueue->GetInvoker(queue);
}

TTransactionManagerPtr TBootstrap::GetTransactionManager() const
{
    return TransactionManager;
}

TSchedulerPtr TBootstrap::GetScheduler() const
{
    return Scheduler;
}

NCellDirectory::TCellDirectoryPtr TBootstrap::GetCellDirectory() const
{
    return CellDirectory;
}

IThroughputThrottlerPtr TBootstrap::GetChunkLocationThrottler() const
{
    return ChunkLocationThrottler;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellScheduler
} // namespace NYT
