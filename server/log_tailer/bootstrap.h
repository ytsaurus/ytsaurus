#pragma once

#include "config.h"
#include "log_rotator.h"

#include <yt/ytlib/monitoring/public.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/http/public.h>

namespace NYT::NLogTailer {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    static constexpr int InterruptionExitCode = 0;

    TBootstrap(
        TLogTailerBootstrapConfigPtr config,
        ui16 monitoringPort);

    void Run();

    const TLogTailerConfigPtr& GetConfig();

    const NApi::NNative::IClientPtr& GetMasterClient() const;

    const IInvokerPtr& GetLogTailerInvoker() const;

    const TLogTailerPtr& GetLogTailer() const;

    void Terminate(int exitCode = 0);

private:
    TLogTailerBootstrapConfigPtr Config_;

    NApi::NNative::IConnectionPtr Connection_;
    NApi::NNative::IClientPtr Client_;

    NConcurrency::TActionQueuePtr LogTailerQueue_;

    NMonitoring::TMonitoringManagerPtr MonitoringManager_;

    NYTree::IMapNodePtr OrchidRoot_;

    NHttp::IServerPtr HttpServer_;

    TLogTailerPtr LogTailer_;

    ui16 MonitoringPort_;

    std::atomic<int> SigintCounter_ = {0};

    void SigintHandler();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogTailer
