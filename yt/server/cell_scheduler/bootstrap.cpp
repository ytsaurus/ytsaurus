#include "stdafx.h"
#include "bootstrap.h"
#include "config.h"

#include <core/misc/address.h>
#include <core/misc/ref_counted_tracker.h>

#include <core/concurrency/action_queue.h>

#include <core/bus/server.h>
#include <core/bus/tcp_server.h>
#include <core/bus/config.h>

#include <core/rpc/server.h>
#include <core/rpc/bus_server.h>
#include <core/rpc/retrying_channel.h>
#include <core/rpc/bus_channel.h>

#include <ytlib/api/connection.h>
#include <ytlib/api/client.h>

#include <ytlib/hydra/peer_channel.h>
#include <ytlib/hydra/config.h>

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
#include <ytlib/transaction_client/timestamp_provider.h>
#include <ytlib/transaction_client/remote_timestamp_provider.h>

#include <ytlib/hive/cell_directory.h>

#include <ytlib/hive/cluster_directory.h>

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
using namespace NHydra;
using namespace NMonitoring;
using namespace NObjectClient;
using namespace NOrchid;
using namespace NProfiling;
using namespace NRpc;
using namespace NScheduler;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NConcurrency;
using namespace NHive;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Bootstrap");

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
    srand(time(nullptr));

    ControlQueue = New<TFairShareActionQueue>("Control", EControlQueue::GetDomainNames());

    auto result = BIND(&TBootstrap::DoRun, this)
        .Guarded()
        .AsyncVia(GetControlInvoker())
        .Run()
        .Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(result);

    Sleep(TDuration::Max());
}

void TBootstrap::DoRun()
{
    LocalAddress = BuildServiceAddress(
        TAddressResolver::Get()->GetLocalHostName(),
        Config->RpcPort);

    LOG_INFO("Starting scheduler (LocalAddress: %s, MasterAddresses: [%s])",
        ~LocalAddress,
        ~JoinToString(Config->ClusterConnection->Master->Addresses));

    auto connection = CreateConnection(Config->ClusterConnection);
    MasterClient = connection->CreateClient();

    BusServer = CreateTcpBusServer(New<TTcpBusServerConfig>(Config->RpcPort));

    RpcServer = CreateBusServer(BusServer);

    HttpServer.reset(new NHttp::TServer(Config->MonitoringPort));

    ClusterDirectory = New<NHive::TClusterDirectory>(MasterClient->GetConnection());

    Scheduler = New<TScheduler>(Config->Scheduler, this);

    auto monitoringManager = New<TMonitoringManager>();
    monitoringManager->Register(
        "/ref_counted",
        TRefCountedTracker::Get()->GetMonitoringProducer());
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

    RpcServer->RegisterService(CreateOrchidService(
        orchidRoot,
        GetControlInvoker()));

    HttpServer->Register(
        "/orchid",
        NMonitoring::GetYPathHttpHandler(orchidRoot));

    RpcServer->RegisterService(CreateSchedulerService(this));
    RpcServer->RegisterService(CreateJobTrackerService(this));

    LOG_INFO("Listening for HTTP requests on port %d", Config->MonitoringPort);
    HttpServer->Start();

    LOG_INFO("Listening for RPC requests on port %d", Config->RpcPort);
    RpcServer->Configure(Config->RpcServer);
    RpcServer->Start();

    Scheduler->Initialize();
}

TCellSchedulerConfigPtr TBootstrap::GetConfig() const
{
    return Config;
}

IClientPtr TBootstrap::GetMasterClient() const
{
    return MasterClient;
}

const Stroka& TBootstrap::GetLocalAddress() const
{
    return LocalAddress;
}

IInvokerPtr TBootstrap::GetControlInvoker(EControlQueue queue) const
{
    return ControlQueue->GetInvoker(queue);
}

TSchedulerPtr TBootstrap::GetScheduler() const
{
    return Scheduler;
}

NHive::TClusterDirectoryPtr TBootstrap::GetClusterDirectory() const
{
    return ClusterDirectory;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellScheduler
} // namespace NYT
