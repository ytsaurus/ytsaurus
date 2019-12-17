#include "bootstrap.h"

#include "log_reader.h"
#include "log_tailer.h"

#include <yt/core/misc/signal_registry.h>

#include <yt/ytlib/api/native/connection.h>

namespace NYT::NLogTailer {

using namespace NApi::NNative;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("Bootstrap");

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(NYT::NLogTailer::TLogTailerBootstrapConfigPtr config)
    : Config_(std::move(config))
    , LogTailerQueue_(New<TActionQueue>())
    , LogTailer_(New<TLogTailer>(this, Config_->LogTailer))
{ }

void TBootstrap::Run()
{
    YT_LOG_INFO("Starting log tailer");

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
        _exit(InterruptionExitCode);
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

void TBootstrap::Terminate()
{
    _exit(0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogTailer
