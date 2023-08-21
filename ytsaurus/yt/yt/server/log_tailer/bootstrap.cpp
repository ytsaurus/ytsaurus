#include "bootstrap.h"

#include "log_reader.h"
#include "log_tailer.h"

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/library/monitoring/http_integration.h>
#include <yt/yt/library/monitoring/monitoring_manager.h>

#include <yt/yt/library/program/build_attributes.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/misc/signal_registry.h>

#include <yt/yt/core/logging/log_manager.h>

namespace NYT::NLogTailer {

using namespace NApi::NNative;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static inline const NLogging::TLogger Logger("Bootstrap");

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

    Sleep(TDuration::Max());
}

void TBootstrap::SigintHandler()
{
    ++SigintCounter_;
    YT_LOG_INFO("Received SIGINT (SigintCount: %v)", static_cast<int>(SigintCounter_));
    if (SigintCounter_ > 1) {
        Abort(InterruptionExitCode);
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
    _exit(exitCode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogTailer
