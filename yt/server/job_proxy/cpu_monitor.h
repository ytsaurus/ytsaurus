#pragma once

#include "public.h"

#include <yt/core/actions/invoker.h>
#include <yt/core/logging/log.h>

#include <util/datetime/base.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ECpuMonitorVote,
    (Increase)
    (Decrease)
    (Keep)
);

class TCpuMonitor
    : public TRefCounted
{
public:
    TCpuMonitor(
        NScheduler::TJobCpuMonitorConfigPtr config,
        IInvokerPtr invoker,
        TJobProxy* jobProxy,
        double hardCpuLimit);

    void Start();
    TFuture<void> Stop();
    void FillStatistics(NJobTrackerClient::TStatistics& statistics) const;

private:
    NScheduler::TJobCpuMonitorConfigPtr Config_;

    NConcurrency::TPeriodicExecutorPtr MonitoringExecutor_;

    TJobProxy* JobProxy_;

    const double HardLimit_;
    double SoftLimit_;
    TNullable<double> SmoothedUsage_;

    TNullable<TInstant> LastCheckTime_;
    TNullable<TDuration> LastTotalCpu_;

    std::deque<ECpuMonitorVote> Votes_;

    NLogging::TLogger Logger;

    bool UpdateSmoothedValue();
    void UpdateVotes();
    TNullable<double> TryMakeDecision();

    void DoCheck();
};

DEFINE_REFCOUNTED_TYPE(TCpuMonitor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
