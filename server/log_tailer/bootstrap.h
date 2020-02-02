#pragma once

#include "config.h"
#include "log_rotator.h"

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/periodic_executor.h>

namespace NYT::NLogTailer {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    static constexpr int InterruptionExitCode = 0;

    TBootstrap(TLogTailerBootstrapConfigPtr config);

    void Run();

    const TLogTailerConfigPtr& GetConfig();

    const NApi::NNative::IClientPtr& GetMasterClient() const;

    const IInvokerPtr& GetLogTailerInvoker() const;

    const TLogTailerPtr& GetLogTailer() const;

    void Terminate();

private:
    TLogTailerBootstrapConfigPtr Config_;

    NApi::NNative::IConnectionPtr Connection_;
    NApi::NNative::IClientPtr Client_;

    NConcurrency::TActionQueuePtr LogTailerQueue_;

    TLogTailerPtr LogTailer_;

    std::atomic<int> SigintCounter_ = {0};

    void SigintHandler();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogTailer
