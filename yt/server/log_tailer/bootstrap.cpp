#include "bootstrap.h"

#include "log_reader.h"
#include "log_tailer.h"

#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/monitoring/http_integration.h>
#include <yt/ytlib/monitoring/monitoring_manager.h>

#include <yt/ytlib/program/build_attributes.h>

#include <yt/core/http/server.h>

#include <yt/core/misc/signal_registry.h>

namespace NYT::NLogTailer {

using namespace NApi::NNative;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("Bootstrap");

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
    NYT::NLogTailer::TLogTailerBootstrapConfigPtr config,
    ui16 monitoringPort)
    : Config_(std::move(config))
    , LogTailerQueue_(New<TActionQueue>("LogTailer"))
    , LogTailer_(New<TLogTailer>(this, Config_->LogTailer))
    , MonitoringPort_(monitoringPort)
{ }

void TBootstrap::Run()
{
    YT_LOG_INFO("Starting log tailer");

    Config_->MonitoringServer->Port = MonitoringPort_;
    Config_->MonitoringServer->ServerName = "monitoring";
    HttpServer_ = NHttp::CreateServer(Config_->MonitoringServer);

    NMonitoring::Initialize(HttpServer_, &MonitoringManager_, &OrchidRoot_);

    SetBuildAttributes(OrchidRoot_, "clickhouse_log_tailer");

    HttpServer_->Start();

    Connection_ = CreateConnection(Config_->ClusterConnection);

    NApi::TClientOptions clientOptions;
    clientOptions.PinnedUser = Config_->ClusterUser;
    Client_ = Connection_->CreateNativeClient(clientOptions);

    LogTailer_->Run();

    // Bootstrap never dies, so it is _kinda_ safe.
    TSignalRegistry::Get()->PushCallback(SIGINT, [=] { SigintHandler(); });

    Sleep(TDuration::Max());
}

void TBootstrap::SigintHandler()
{
    ++SigintCounter_;
    YT_LOG_INFO("Received SIGINT (SigintCount: %v)", static_cast<int>(SigintCounter_));
    if (SigintCounter_ > 1) {
        Terminate(InterruptionExitCode);
    }
    YT_LOG_INFO("Ignoring first SIGINT");
}

const TLogTailerConfigPtr& TBootstrap::GetConfig()
{
    return Config_->LogTailer;
}

const IClientPtr& TBootstrap::GetMasterClient() const
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

void TBootstrap::Terminate(int exitCode)
{
    MonitoringManager_->Stop();
    HttpServer_->Stop();
    _exit(exitCode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogTailer
