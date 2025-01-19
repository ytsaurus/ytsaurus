#pragma once

#include "config.h"
#include "log_rotator.h"

#include <yt/yt/library/monitoring/public.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/http/public.h>

namespace NYT::NLogTailer {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    explicit TBootstrap(TLogTailerBootstrapConfigPtr config);

    void Run();

    const TLogTailerConfigPtr& GetConfig();

    const NApi::NNative::IClientPtr& GetClient() const;

    const IInvokerPtr& GetLogTailerInvoker() const;

    const TLogTailerPtr& GetLogTailer() const;

    [[noreturn]] void Abort(int exitCode);

private:
    const TLogTailerBootstrapConfigPtr Config_;

    NApi::NNative::IConnectionPtr Connection_;
    NApi::NNative::IClientPtr Client_;

    NConcurrency::TActionQueuePtr LogTailerQueue_;

    NMonitoring::IMonitoringManagerPtr MonitoringManager_;

    NYTree::IMapNodePtr OrchidRoot_;

    NHttp::IServerPtr HttpServer_;

    TLogTailerPtr LogTailer_;

    std::atomic<int> SigintCounter_ = 0;

    void SigintHandler();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogTailer
