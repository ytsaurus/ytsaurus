#include "bootstrap.h"

#include "stack_size_checker.h"
#include "host.h"
#include "clickhouse_server.h"
#include "clickhouse_service.h"
#include "query_registry.h"
#include "config.h"

#include <yt/server/lib/admin/admin_service.h>
#include <yt/server/lib/core_dump/core_dumper.h>

#include <yt/ytlib/monitoring/http_integration.h>
#include <yt/ytlib/monitoring/monitoring_manager.h>

#include <yt/ytlib/program/build_attributes.h>

#include <yt/ytlib/orchid/orchid_service.h>

#include <yt/core/bus/tcp/server.h>
#include <yt/core/misc/crash_handler.h>

#include <yt/core/logging/log_manager.h>

#include <yt/core/misc/signal_registry.h>

#include <yt/core/http/server.h>

#include <yt/core/rpc/bus/server.h>

#include <yt/core/ytree/virtual.h>

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

static const auto& Logger = ClickHouseYtLogger;

////////////////////////////////////////////////////////////////////////////////

// See the comment in stack_size_checker.h.
static const auto UnusedValue = IgnoreMe();

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
    TClickHouseServerBootstrapConfigPtr config,
    INodePtr configNode)
    : Config_(std::move(config))
    , ConfigNode_(std::move(configNode))
{
    WarnForUnrecognizedOptions(Logger, Config_);
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
    YT_LOG_INFO("Starting ClickHouse server");

    // Make RSS predictable.
    NYTAlloc::SetEnableEagerMemoryRelease(true);

    // Set up crash handlers.
    TSignalRegistry::Get()->PushCallback(AllCrashSignals, [this] { HandleCrashSignal(); });
    TSignalRegistry::Get()->PushCallback(AllCrashSignals, CrashSignalHandler);
    TSignalRegistry::Get()->PushDefaultSignalHandler(AllCrashSignals);

    Config_->MonitoringServer->ServerName = "monitoring";
    HttpServer_ = NHttp::CreateServer(Config_->MonitoringServer);

    NYTree::IMapNodePtr orchidRoot;
    NMonitoring::Initialize(HttpServer_, &MonitoringManager_, &orchidRoot);

    SetNodeByYPath(
        orchidRoot,
        "/config",
        ConfigNode_);
    SetBuildAttributes(orchidRoot, "clickhouse_server");

    if (Config_->CoreDumper) {
        CoreDumper_ = NCoreDump::CreateCoreDumper(Config_->CoreDumper);
    }

    BusServer_ = CreateTcpBusServer(Config_->BusServer);

    RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);

    RpcServer_->RegisterService(CreateAdminService(
        GetControlInvoker(),
        CoreDumper_));

    RpcServer_->RegisterService(CreateOrchidService(
        orchidRoot,
        GetControlInvoker()));

    RpcServer_->Configure(Config_->RpcServer);

    {
        auto host = New<THost>(GetControlInvoker(), Config_->GetPorts(), Config_->Yt, Config_->ClusterConnection);
        ClickHouseServer_ = CreateClickHouseServer(host.Get(), Config_->ClickHouse);

        host->SetContext(ClickHouseServer_->GetContext());

        // NB: Host_ should be set only after it becomes ready to handle crash signals.
        Host_ = std::move(host);
    }

    RpcServer_->RegisterService(CreateClickHouseService(Host_.Get()));

    SetNodeByYPath(
        orchidRoot,
        "/queries",
        CreateVirtualNode(Host_->GetQueryRegistry()->GetOrchidService()));

    YT_LOG_INFO("Monitoring HTTP server set up (Port: %v)", Config_->MonitoringPort);
    HttpServer_->Start();

    YT_LOG_INFO("RPC server set up (Port: %v)", Config_->RpcPort);
    RpcServer_->Start();

    ClickHouseServer_->Start();

    Host_->Start();

    // Bootstrap never dies, so it is _kinda_ safe.
    TSignalRegistry::Get()->PushCallback(SIGINT, [=] { HandleSigint(); });
}

const IInvokerPtr& TBootstrap::GetControlInvoker() const
{
    return ControlQueue_->GetInvoker();
}

void TBootstrap::HandleCrashSignal()
{
    // There is a data race in this check, but we are ok with that.
    if (Host_) {
        Host_->HandleCrashSignal();
    }
}

void TBootstrap::HandleSigint()
{
    ++SigintCounter_;
    if (Host_) {
        Host_->HandleSigint();
    }
    if (SigintCounter_ > 1) {
        _exit(InterruptionExitCode);
    }
    YT_LOG_INFO("Stopping server due to SIGINT");
    TFuture<void> discoveryStopFuture;
    if (Host_) {
        discoveryStopFuture = Host_->StopDiscovery();
    } else {
        YT_LOG_INFO("Host is not set up");
        discoveryStopFuture = VoidFuture;
    }
    discoveryStopFuture.Apply(BIND([this] {
        TDelayedExecutor::WaitForDuration(Config_->InterruptionGracefulTimeout);
        if (Host_) {
            Y_UNUSED(WaitFor(Host_->GetIdleFuture()));
        }
        if (ClickHouseServer_) {
            ClickHouseServer_->Stop();
        }
        if (RpcServer_) {
            Y_UNUSED(WaitFor(RpcServer_->Stop()));
        }
        if (MonitoringManager_) {
            MonitoringManager_->Stop();
        }
        if (HttpServer_) {
            HttpServer_->Stop();
        }
        NLogging::TLogManager::StaticShutdown();
        _exit(InterruptionExitCode);
    }).Via(GetControlInvoker()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
