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

    const IInvokerPtr& GetLogWriterLivenessCheckerInvoker() const;
    const IInvokerPtr& GetRotatorInvoker() const;
    const IInvokerPtr& GetReaderInvoker() const;

    const TLogTailerPtr& GetLogTailer() const;

    void Terminate();

private:
    TLogTailerBootstrapConfigPtr Config_;

    NApi::NNative::IConnectionPtr Connection_;
    NApi::NNative::IClientPtr Client_;

    NConcurrency::TActionQueuePtr LogWriterLivenessCheckerQueue_;
    NConcurrency::TActionQueuePtr RotatorQueue_;
    NConcurrency::TActionQueuePtr ReaderQueue_;

    TLogTailerPtr LogTailer_;

    std::atomic<int> SigintCounter_ = {0};

    void SigintHandler();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogTailer
