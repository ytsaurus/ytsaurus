#include "stdafx.h"
#include "scheduler_bootstrap.h"

#include <ytlib/misc/ref_counted_tracker.h>

#include <ytlib/actions/action_queue.h>

#include <ytlib/bus/nl_server.h>

#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/virtual.h>

#include <ytlib/orchid/orchid_service.h>

#include <ytlib/monitoring/monitoring_manager.h>
#include <ytlib/monitoring/ytree_integration.h>
#include <ytlib/monitoring/http_server.h>
#include <ytlib/monitoring/http_integration.h>
#include <ytlib/monitoring/statlog.h>

#include <ytlib/ytree/yson_file_service.h>
#include <ytlib/ytree/ypath_client.h>

namespace NYT {

static NLog::TLogger Logger("Server");

using NBus::IBusServer;
using NBus::TNLBusServerConfig;
using NBus::CreateNLBusServer;

using NRpc::IServer;
using NRpc::CreateRpcServer;

using namespace NYTree;

using NMonitoring::TMonitoringManager;
using NMonitoring::GetYPathHttpHandler;
using NMonitoring::CreateMonitoringProducer;

using NOrchid::TOrchidService;

////////////////////////////////////////////////////////////////////////////////

TSchedulerBootstrap::TSchedulerBootstrap(
    const Stroka& configFileName,
    TConfig* config)
    : ConfigFileName(configFileName)
    , Config(config)
{ }

void TSchedulerBootstrap::Run()
{
    LOG_INFO("Starting scheduler");

    auto controlQueue = New<TActionQueue>("Control");

    auto busServer = CreateNLBusServer(~New<TNLBusServerConfig>(Config->RpcPort));

    auto rpcServer = CreateRpcServer(~busServer);

    auto monitoringManager = New<TMonitoringManager>();
    monitoringManager->Register(
        "ref_counted",
        FromMethod(&TRefCountedTracker::GetMonitoringInfo, TRefCountedTracker::Get()));
    monitoringManager->Register(
        "bus_server",
        FromMethod(&IBusServer::GetMonitoringInfo, busServer));
    monitoringManager->Register(
        "rpc_server",
        FromMethod(&IServer::GetMonitoringInfo, rpcServer));
    monitoringManager->Start();

    auto orchidFactory = GetEphemeralNodeFactory();
    auto orchidRoot = orchidFactory->CreateMap();
    SyncYPathSetNode(
        ~orchidRoot,
        "monitoring",
        ~CreateVirtualNode(~CreateMonitoringProducer(~monitoringManager)));
    SyncYPathSetNode(
        ~orchidRoot,
        "config",
        ~CreateVirtualNode(~CreateYsonFileProducer(ConfigFileName)));

    auto orchidService = New<TOrchidService>(
        ~orchidRoot,
        ~controlQueue->GetInvoker());

    rpcServer->RegisterService(~orchidService);

    THolder<NHttp::TServer> httpServer(new NHttp::TServer(Config->MonitoringPort));
	httpServer->Register(
		"/orchid",
		~NMonitoring::GetYPathHttpHandler(~orchidRoot->Via(~controlQueue->GetInvoker())));

    LOG_INFO("Listening for HTTP requests on port %d", Config->MonitoringPort);
    httpServer->Start();

    LOG_INFO("Listening for RPC requests on port %d", Config->RpcPort);
    rpcServer->Start();

    Sleep(TDuration::Max());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
