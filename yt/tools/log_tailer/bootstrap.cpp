#include "bootstrap.h"

#include "log_reader.h"
#include "log_tailer.h"

#include <yt/ytlib/api/native/connection.h>

namespace NYT::NLogTailer {

using namespace NApi::NNative;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("Bootstrap");

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(NYT::NLogTailer::TLogTailerBootstrapConfigPtr config)
    : Config_(std::move(config))
    , RotatorQueue_(New<TActionQueue>())
    , ReaderQueue_(New<TActionQueue>())
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

    Sleep(TDuration::Max());
}

const TLogTailerConfigPtr& TBootstrap::GetConfig()
{
    return Config_->LogTailer;
}

const IClientPtr& TBootstrap::GetMasterClient() const
{
    return Client_;
}

const IInvokerPtr& TBootstrap::GetReaderInvoker() const
{
    return ReaderQueue_->GetInvoker();
}

const IInvokerPtr& TBootstrap::GetRotatorInvoker() const
{
    return RotatorQueue_->GetInvoker();
}

const TLogTailerPtr& TBootstrap::GetLogTailer() const
{
    return LogTailer_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogTailer
