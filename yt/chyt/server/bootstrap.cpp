#include "bootstrap.h"

#include "launcher_compatibility.h"
#include "stack_size_checker.h"
#include "host.h"
#include "clickhouse_server.h"
#include "clickhouse_service.h"
#include "query_registry.h"
#include "config.h"

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/library/monitoring/http_integration.h>
#include <yt/yt/library/monitoring/monitoring_manager.h>

#include <yt/yt/library/program/build_attributes.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/misc/crash_handler.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/misc/signal_registry.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/ytree/virtual.h>

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

    try {
        BIND(&TBootstrap::DoRun, this)
            .AsyncVia(GetControlInvoker())
            .Run()
            .Get()
            .ThrowOnError();
    } catch (std::exception& ex) {
        // Make best-effort check that error is an "Address already in use" error.
        // There is no way to deterministically prevent that in some environments
        // (like dist build or local run), so we just perform graceful exit in this case.
        auto what = TString(ex.what());
        TString AddressAlreadyInUse = "Address already in use";
        if (what.find(AddressAlreadyInUse) != TString::npos) {
            _exit(0);
        } else {
            throw;
        }
    }

    Sleep(TDuration::Max());
}

void TBootstrap::DoRun()
{
    YT_LOG_INFO("Starting ClickHouse server");

    // TODO(gepardo): Add authentication here.

    ValidateLauncherCompatibility(Config_->Launcher);

    // Set up crash handlers.
    TSignalRegistry::Get()->PushCallback(AllCrashSignals, [this] { HandleCrashSignal(); });
    TSignalRegistry::Get()->PushCallback(AllCrashSignals, CrashSignalHandler);
    TSignalRegistry::Get()->PushDefaultSignalHandler(AllCrashSignals);

    HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

    NYTree::IMapNodePtr orchidRoot;
    NMonitoring::Initialize(
        HttpServer_,
        Config_->SolomonExporter,
        &MonitoringManager_,
        &orchidRoot);

    SetNodeByYPath(
        orchidRoot,
        "/config",
        ConfigNode_);
    SetBuildAttributes(
        orchidRoot,
        "clickhouse_server");

    if (Config_->CoreDumper) {
        CoreDumper_ = NCoreDump::CreateCoreDumper(Config_->CoreDumper);
    }

    BusServer_ = CreateBusServer(Config_->BusServer);

    RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);

    RpcServer_->RegisterService(CreateAdminService(
        GetControlInvoker(),
        CoreDumper_,
        /*authenticator*/ nullptr));
    RpcServer_->RegisterService(CreateOrchidService(
        orchidRoot,
        GetControlInvoker(),
        /*authenticator*/ nullptr));

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
    TSignalRegistry::Get()->PushCallback(SIGINT, [this] { HandleSigint(); });
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
        WriteToStderr("*** Immediately stopping server due to second SIGINT ***\n");
        _exit(GracefulInterruptionExitCode);
    }
    WriteToStderr("*** Gracefully stopping server due to SIGINT ***\n");
    YT_LOG_INFO("Stopping server due to SIGINT");

    // Set up hard timeout to avoid hanging in interruption.
    TDelayedExecutor::Submit(
        BIND([] {
            WriteToStderr("*** Interuption timed out ***\n");
            _exit(InterruptionTimedOutExitCode);
        }),
        Config_->InterruptionTimeout,
        GetControlInvoker());

    TFuture<void> discoveryStopFuture;
    if (Host_) {
        discoveryStopFuture = Host_->StopDiscovery();
    } else {
        YT_LOG_INFO("Host is not set up");
        discoveryStopFuture = VoidFuture;
    }
    YT_UNUSED_FUTURE(discoveryStopFuture.Apply(BIND([this] {
        TDelayedExecutor::WaitForDuration(Config_->GracefulInterruptionDelay);
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
        NLogging::TLogManager::Get()->Shutdown();
        WriteToStderr("*** Server gracefully stopped ***\n");
        _exit(GracefulInterruptionExitCode);
    }).Via(GetControlInvoker())));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
