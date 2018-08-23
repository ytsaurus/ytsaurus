#include "bootstrap.h"

#include <yt/server/clickhouse/engine/server.h>
#include <yt/server/clickhouse/server/client_cache.h>
#include <yt/server/clickhouse/server/config.h>
#include <yt/server/clickhouse/server/directory.h>
#include <yt/server/clickhouse/server/logger.h>
#include <yt/server/clickhouse/server/storage.h>

#include <yt/server/admin_server/admin_service.h>

#include <yt/ytlib/program/build_attributes.h>
#include <yt/ytlib/program/configure_singletons.h>
#include <yt/ytlib/api/connection.h>
#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/monitoring/http_integration.h>
#include <yt/ytlib/monitoring/monitoring_manager.h>
#include <yt/ytlib/orchid/orchid_service.h>
#include <yt/ytlib/core_dump/core_dumper.h>

#include <yt/core/bus/tcp/server.h>
#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/throughput_throttler.h>
#include <yt/core/concurrency/thread_pool_poller.h>
#include <yt/core/misc/core_dumper.h>
#include <yt/core/misc/lfalloc_helpers.h>
#include <yt/core/misc/ref_counted_tracker_statistics_producer.h>
#include <yt/core/profiling/profile_manager.h>
#include <yt/core/http/server.h>
#include <yt/core/rpc/bus/server.h>
#include <yt/core/ytree/ephemeral_node_factory.h>
#include <yt/core/ytree/virtual.h>
#include <yt/core/ytree/ypath_client.h>

#include <util/datetime/base.h>

namespace NYT {
namespace NClickHouse {

using namespace NYT::NAdmin;
using namespace NYT::NApi;
using namespace NYT::NBus;
using namespace NYT::NConcurrency;
using namespace NYT::NMonitoring;
using namespace NYT::NOrchid;
using namespace NYT::NProfiling;
using namespace NYT::NRpc;
using namespace NYT::NYTree;

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger EngineLogger("Engine");
const NLogging::TLogger BootstrapLogger("Bootstrap");

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TConfigPtr config,
                       INodePtr configNode,
                       TString xmlConfig,
                       TString instanceId,
                       TString cliqueId,
                       ui16 rpcPort,
                       ui16 monitoringPort,
                       ui16 tcpPort,
                       ui16 httpPort)
    : Config(std::move(config))
    , ConfigNode(std::move(configNode))
    , XmlConfig(std::move(xmlConfig))
    , InstanceId_(std::move(instanceId))
    , CliqueId_(std::move(cliqueId))
    , RpcPort_(rpcPort)
    , MonitoringPort_(monitoringPort)
    , TcpPort_(tcpPort)
    , HttpPort_(httpPort)
{}

TBootstrap::~TBootstrap()
{}

void TBootstrap::Initialize()
{
    ConfigureSingletons(Config);

    ControlQueue = New<TActionQueue>("Control");
    BIND(&TBootstrap::DoInitialize, this)
        .AsyncVia(GetControlInvoker())
        .Run()
        .Get()
        .ThrowOnError();
}

void TBootstrap::Run()
{
    Y_VERIFY(ControlQueue);
    GetControlInvoker()->Invoke(BIND(&TBootstrap::DoRun, this));
}

void TBootstrap::DoInitialize()
{
    MonitoringManager = New<TMonitoringManager>();
    MonitoringManager->Register(
        "/ref_counted",
        CreateRefCountedTrackerStatisticsProducer());

    auto orchidRoot = GetEphemeralNodeFactory(true)->CreateMap();
    SetNodeByYPath(
        orchidRoot,
        "/config",
        ConfigNode);
    SetNodeByYPath(
        orchidRoot,
        "/profiling",
        CreateVirtualNode(TProfileManager::Get()->GetService()));
    SetNodeByYPath(
        orchidRoot,
        "/monitoring",
        CreateVirtualNode(MonitoringManager->GetService()));

    SetBuildAttributes(orchidRoot, "clickhouse-server");

    LFAllocProfiler = std::make_unique<NLFAlloc::TLFAllocProfiler>();

    if (Config->CoreDumper) {
        CoreDumper = NCoreDump::CreateCoreDumper(Config->CoreDumper);
    }

    if (Config->RpcServer) {
        Config->BusServer->Port = RpcPort_;
        BusServer = CreateTcpBusServer(Config->BusServer);

        RpcServer = NRpc::NBus::CreateBusServer(BusServer);

        RpcServer->RegisterService(CreateAdminService(
            GetControlInvoker(),
            CoreDumper));

        RpcServer->RegisterService(CreateOrchidService(
            orchidRoot,
            GetControlInvoker()));

        RpcServer->Configure(Config->RpcServer);
    }

    auto poller = CreateThreadPoolPoller(1, "Http");
    HttpServer = NHttp::CreateServer(MonitoringPort_, poller);

// TODO(prime@): uncomment after arcadia sync
//        HttpServer->AddHandler(
//            "/orchid/",
//            GetOrchidYPathHttpHandler(orchidRoot));

    NNative::TConnectionOptions connectionOptions;
    connectionOptions.RetryRequestQueueSizeLimitExceeded = true;

    Connection = NNative::CreateConnection(
        Config->ClusterConnection,
        connectionOptions);

    NativeClientCache = CreateNativeClientCache(
        Config->ClientCache,
        Connection);

    if (Config->ScanThrottler) {
        ScanThrottler = CreateReconfigurableThroughputThrottler(Config->ScanThrottler);
    }  else {
        ScanThrottler = GetUnlimitedThrottler();
    }

    auto logger = CreateLogger(EngineLogger);

    Storage = CreateStorage(Connection, NativeClientCache, ScanThrottler);

    CoordinationService = CreateCoordinationService(Connection, CliqueId_);

    Server = CreateServer(
        logger,
        Storage,
        CoordinationService,
        XmlConfig,
        CliqueId_,
        InstanceId_,
        TcpPort_,
        HttpPort_);
}

void TBootstrap::DoRun()
{
    const auto& Logger = BootstrapLogger;

    if (MonitoringManager) {
        MonitoringManager->Start();
    }

    if (HttpServer) {
        LOG_INFO("Listening for HTTP requests on port %v", Config->MonitoringPort);
        HttpServer->Start();
    }

    if (RpcServer) {
        LOG_INFO("Listening for RPC requests on port %v", Config->RpcPort);
        RpcServer->Start();
    }

    Server->Start();
}

TConfigPtr TBootstrap::GetConfig() const
{
    return Config;
}

IInvokerPtr TBootstrap::GetControlInvoker() const
{
    return ControlQueue->GetInvoker();
}

NNative::IConnectionPtr TBootstrap::GetConnection() const
{
    return Connection;
}

IThroughputThrottlerPtr TBootstrap::GetScanThrottler() const
{
    return ScanThrottler;
}

}   // namespace NClickHouse
}   // namespace NYT
