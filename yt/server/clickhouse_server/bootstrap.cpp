#include "bootstrap.h"

#include <yt/server/clickhouse_server/server.h>

#include <yt/server/clickhouse_server/client_cache.h>
#include <yt/server/clickhouse_server/config.h>
#include <yt/server/clickhouse_server/directory.h>
#include <yt/server/clickhouse_server/logger.h>
#include <yt/server/clickhouse_server/storage.h>
#include <yt/server/clickhouse_server/clique_authorization_manager.h>

#include <yt/server/lib/admin/admin_service.h>

#include <yt/ytlib/program/build_attributes.h>
#include <yt/ytlib/program/configure_singletons.h>
#include <yt/ytlib/api/connection.h>
#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/monitoring/http_integration.h>
#include <yt/ytlib/monitoring/monitoring_manager.h>
#include <yt/ytlib/orchid/orchid_service.h>
#include <yt/ytlib/core_dump/core_dumper.h>

#include <yt/client/api/client.h>

#include <yt/core/bus/tcp/server.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/throughput_throttler.h>
#include <yt/core/concurrency/thread_pool_poller.h>

#include <yt/core/misc/core_dumper.h>
#include <yt/core/misc/ref_counted_tracker_statistics_producer.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/http/server.h>

#include <yt/core/rpc/bus/server.h>

#include <yt/core/ytree/ephemeral_node_factory.h>
#include <yt/core/ytree/virtual.h>
#include <yt/core/ytree/ypath_client.h>

#include <util/datetime/base.h>

namespace NYT::NClickHouseServer {

using namespace NAdmin;
using namespace NApi;
using namespace NApi::NNative;
using namespace NBus;
using namespace NConcurrency;
using namespace NMonitoring;
using namespace NOrchid;
using namespace NProfiling;
using namespace NRpc;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger EngineLogger("Engine");
const NLogging::TLogger BootstrapLogger("Bootstrap");

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
    TClickHouseServerBootstrapConfigPtr config,
    INodePtr configNode,
    TString instanceId,
    TString cliqueId,
    ui16 rpcPort,
    ui16 monitoringPort,
    ui16 tcpPort,
    ui16 httpPort)
    : Config_(std::move(config))
    , ConfigNode_(std::move(configNode))
    , InstanceId_(std::move(instanceId))
    , CliqueId_(std::move(cliqueId))
    , RpcPort_(rpcPort)
    , MonitoringPort_(monitoringPort)
    , TcpPort_(tcpPort)
    , HttpPort_(httpPort)
{
    WarnForUnrecognizedOptions(BootstrapLogger, Config_);
}

void TBootstrap::Run()
{
    ControlQueue_ = New<TActionQueue>("Control");

    BIND(&TBootstrap::DoRun, this)
        .AsyncVia(GetControlInvoker())
        .Run()
        .Get()
        .ThrowOnError();

    Sleep(TDuration::Max());
}

void TBootstrap::DoRun()
{
    MonitoringManager = New<TMonitoringManager>();
    MonitoringManager->Register(
        "/ref_counted",
        CreateRefCountedTrackerStatisticsProducer());

    auto orchidRoot = GetEphemeralNodeFactory(true)->CreateMap();
    SetNodeByYPath(
        orchidRoot,
        "/config",
        ConfigNode_);
    SetNodeByYPath(
        orchidRoot,
        "/profiling",
        CreateVirtualNode(TProfileManager::Get()->GetService()));
    SetNodeByYPath(
        orchidRoot,
        "/monitoring",
        CreateVirtualNode(MonitoringManager->GetService()));

    SetBuildAttributes(orchidRoot, "clickhouse_server");

    if (Config_->CoreDumper) {
        CoreDumper = NCoreDump::CreateCoreDumper(Config_->CoreDumper);
    }

    if (Config_->RpcServer) {
        Config_->BusServer->Port = RpcPort_;
        BusServer = CreateTcpBusServer(Config_->BusServer);

        RpcServer = NRpc::NBus::CreateBusServer(BusServer);

        RpcServer->RegisterService(CreateAdminService(
            GetControlInvoker(),
            CoreDumper));

        RpcServer->RegisterService(CreateOrchidService(
            orchidRoot,
            GetControlInvoker()));

        RpcServer->Configure(Config_->RpcServer);
    }

    auto poller = CreateThreadPoolPoller(1, "Http");
    HttpServer = NHttp::CreateServer(MonitoringPort_, poller);

    HttpServer->AddHandler(
        "/orchid/",
        GetOrchidYPathHttpHandler(orchidRoot));

    NApi::NNative::TConnectionOptions connectionOptions;
    connectionOptions.RetryRequestQueueSizeLimitExceeded = true;

    Connection = NApi::NNative::CreateConnection(
        Config_->ClusterConnection,
        connectionOptions);

    NativeClientCache = CreateNativeClientCache(
        Config_->ClientCache,
        Connection);

    if (Config_->ScanThrottler) {
        ScanThrottler = CreateReconfigurableThroughputThrottler(Config_->ScanThrottler);
    }  else {
        ScanThrottler = GetUnlimitedThrottler();
    }

    auto logger = CreateLogger(EngineLogger);

    Storage = CreateStorage(Connection, NativeClientCache, ScanThrottler);

    CoordinationService = CreateCoordinationService(Connection, CliqueId_);

    auto client = NativeClientCache->CreateNativeClient(TClientOptions("root"));
    CliqueAuthorizationManager = CreateCliqueAuthorizationManager(client, CliqueId_, Config_->ValidateOperationPermission);

    Server = std::make_unique<TServer>(
        logger,
        Storage,
        CoordinationService,
        CliqueAuthorizationManager,
        Config_,
        CliqueId_,
        InstanceId_,
        TcpPort_,
        HttpPort_);

    const auto& Logger = BootstrapLogger;

    if (MonitoringManager) {
        MonitoringManager->Start();
    }

    if (HttpServer) {
        YT_LOG_INFO("Listening for HTTP requests on port %v", Config_->MonitoringPort);
        HttpServer->Start();
    }

    if (RpcServer) {
        YT_LOG_INFO("Listening for RPC requests on port %v", Config_->RpcPort);
        RpcServer->Start();
    }

    Server->Start();
}

TClickHouseServerBootstrapConfigPtr TBootstrap::GetConfig() const
{
    return Config_;
}

IInvokerPtr TBootstrap::GetControlInvoker() const
{
    return ControlQueue_->GetInvoker();
}

NApi::NNative::IConnectionPtr TBootstrap::GetConnection() const
{
    return Connection;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
