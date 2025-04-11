#include "bootstrap.h"

#include "log_reader.h"
#include "log_tailer.h"

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/library/monitoring/http_integration.h>
#include <yt/yt/library/monitoring/monitoring_manager.h>

#include <yt/yt/library/program/build_attributes.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/library/signals/signal_registry.h>

#include <library/cpp/yt/system/exit.h>

namespace NYT::NLogTailer {

using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NSignals;

////////////////////////////////////////////////////////////////////////////////

static YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Bootstrap");

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TLogTailerBootstrapConfigPtr config)
    : Config_(std::move(config))
    , LogTailerQueue_(New<TActionQueue>("LogTailer"))
    , LogTailer_(New<TLogTailer>(this, Config_->LogTailer))
{ }

void TBootstrap::Run()
{
    YT_LOG_INFO("Starting log tailer");

    HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

    NMonitoring::Initialize(
        HttpServer_,
        Config_->SolomonExporter,
        &MonitoringManager_,
        &OrchidRoot_);

    SetBuildAttributes(OrchidRoot_, "clickhouse_log_tailer");

    HttpServer_->Start();

    Connection_ = CreateConnection(Config_->ClusterConnection);

    auto clientOptions = NApi::TClientOptions::FromUser(Config_->ClusterUser);
    Client_ = Connection_->CreateNativeClient(clientOptions);

    LogTailer_->Run();

    // Bootstrap never dies, so it is _kinda_ safe.
    TSignalRegistry::Get()->PushCallback(SIGINT, [this] { SigintHandler(); });
}

void TBootstrap::SigintHandler()
{
    auto counter = ++SigintCounter_;
    YT_LOG_INFO("Received SIGINT (SigintCount: %v)", counter);
    if (counter > 1) {
        Abort(ToUnderlying(EProcessExitCode::OK));
    }
    YT_LOG_INFO("Ignoring first SIGINT");
}

const TLogTailerConfigPtr& TBootstrap::GetConfig()
{
    return Config_->LogTailer;
}

const IClientPtr& TBootstrap::GetClient() const
{
    return Client_;
}

const IInvokerPtr& TBootstrap::GetLogTailerInvoker() const
{
    return LogTailerQueue_->GetInvoker();
}

const TLogTailerPtr& TBootstrap::GetLogTailer() const
{
    return LogTailer_;
}

void TBootstrap::Abort(int exitCode)
{
    NLogging::TLogManager::Get()->Shutdown();
    MonitoringManager_->Stop();
    HttpServer_->Stop();
    AbortProcessSilently(exitCode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogTailer
