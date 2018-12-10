#include "bootstrap.h"

#include "config.h"
#include "private.h"
#include "clickhouse_proxy.h"

#include <yt/server/admin_server/admin_service.h>

#include <yt/ytlib/program/build_attributes.h>

#include <yt/ytlib/monitoring/http_integration.h>
#include <yt/ytlib/monitoring/monitoring_manager.h>

#include <yt/ytlib/auth/authentication_manager.h>

#include <yt/core/bus/server.h>

#include <yt/core/bus/tcp/config.h>
#include <yt/core/bus/tcp/server.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/thread_pool_poller.h>

#include <yt/core/net/address.h>

#include <yt/core/misc/core_dumper.h>
#include <yt/core/misc/ref_counted_tracker.h>
#include <yt/core/misc/ref_counted_tracker_statistics_producer.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/ytree/virtual.h>

#include <yt/core/http/server.h>

namespace NYT {
namespace NClickHouseProxy {

using namespace NAdmin;
using namespace NBus;
using namespace NMonitoring;
using namespace NProfiling;
using namespace NRpc;
using namespace NYTree;
using namespace NConcurrency;
using namespace NApi;
using namespace NAuth;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ClickHouseProxyLogger;

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TClickHouseProxyServerConfigPtr config, INodePtr configNode)
    : Config_(std::move(config))
    , ConfigNode_(std::move(configNode))
    , ControlQueue_(New<TActionQueue>("Control"))
    , WorkerPool_(New<TThreadPool>(Config_->WorkerThreadPoolSize, "Worker"))
    , HttpPoller_(CreateThreadPoolPoller(1, "HttpPoller"))
{
    WarnForUnrecognizedOptions(Logger, Config_);
}

TBootstrap::~TBootstrap() = default;

void TBootstrap::Run()
{
    BIND(&TBootstrap::DoRun, this)
        .AsyncVia(ControlQueue_->GetInvoker())
        .Run()
        .Get()
        .ThrowOnError();

    Sleep(TDuration::Max());
}

void TBootstrap::DoRun()
{
    LOG_INFO("Starting Clickhouse proxy");

    AuthenticationManager_ = New<TAuthenticationManager>(
        Config_->AuthenticationManager,
        HttpPoller_,
        nullptr /* client */);
    TokenAuthenticator_ = AuthenticationManager_->GetTokenAuthenticator();

    Config_->MonitoringServer->Port = Config_->MonitoringPort;
    Config_->MonitoringServer->BindRetryCount = Config_->BusServer->BindRetryCount;
    Config_->MonitoringServer->BindRetryBackoff = Config_->BusServer->BindRetryBackoff;
    MonitoringHttpServer_ = NHttp::CreateServer(
        Config_->MonitoringServer);

    MonitoringManager_ = New<TMonitoringManager>();
    MonitoringManager_->Register(
        "/ref_counted",
        CreateRefCountedTrackerStatisticsProducer());
    MonitoringManager_->Start();

    auto orchidRoot = NYTree::GetEphemeralNodeFactory(true)->CreateMap();
    SetNodeByYPath(
        orchidRoot,
        "/monitoring",
        CreateVirtualNode(MonitoringManager_->GetService()));
    SetNodeByYPath(
        orchidRoot,
        "/profiling",
        CreateVirtualNode(TProfileManager::Get()->GetService()));
    SetNodeByYPath(
        orchidRoot,
        "/config",
        ConfigNode_);

    SetBuildAttributes(orchidRoot, "proxy");

    MonitoringHttpServer_->AddHandler(
        "/orchid/",
        NMonitoring::GetOrchidYPathHttpHandler(orchidRoot));

    LOG_INFO("Listening for monitoring HTTP requests on port %v", Config_->MonitoringPort);
    MonitoringHttpServer_->Start();

    ClickHouseProxy_ = New<TClickHouseProxyHandler>(Config_->ClickHouseProxy, this /* bootstrap */);

    LOG_INFO("Listening for clickhouse HTTP requests on port %v", Config_->ClickHouseProxyHttpServer->Port);
    ClickHouseProxyServer_ = NHttp::CreateServer(Config_->ClickHouseProxyHttpServer);
    ClickHouseProxyServer_->AddHandler("/", ClickHouseProxy_);

    ClickHouseProxyServer_->Start();
}

const TClickHouseProxyServerConfigPtr& TBootstrap::GetConfig() const
{
    return Config_;
}

const IInvokerPtr& TBootstrap::GetControlInvoker() const
{
    return ControlQueue_->GetInvoker();
}

const IInvokerPtr& TBootstrap::GetWorkerInvoker() const
{
    return WorkerPool_->GetInvoker();
}

const NAuth::ITokenAuthenticatorPtr& TBootstrap::GetTokenAuthenticator() const
{
    return TokenAuthenticator_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NClickHouseProxy
} // namespace NYT
