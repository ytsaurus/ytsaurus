#include "cpu_monitor.h"
#include "job_proxy.h"

#include <yt/core/concurrency/periodic_executor.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

TCpuMonitor::TCpuMonitor(
    TJobCpuMonitorConfigPtr config,
    IInvokerPtr invoker,
    double hardCpuLimit,
    TJobProxy* jobProxy)
    : HardLimit_(hardCpuLimit)
    , SoftLimit_(hardCpuLimit)
    , Config_(std::move(config))
    , MonitoringExecutor_(New<NConcurrency::TPeriodicExecutor>(
        invoker,
        BIND(&TCpuMonitor::DoCheck, MakeWeak(this)),
        Config_->CheckPeriod))
    , JobProxy_(jobProxy)
    , Logger("CpuMonitor")
{ }

void TCpuMonitor::Start()
{
    MonitoringExecutor_->Start();
}

TFuture<void> TCpuMonitor::Stop()
{
    return MonitoringExecutor_->Stop();
}

void TCpuMonitor::FillStatistics(NJobTrackerClient::TStatistics& statistics) const
{
    if (SmoothedUsage_) {
        statistics.AddSample("/job_proxy/smoothed_cpu_usage_x100", static_cast<i64>(*SmoothedUsage_ * 100));
        statistics.AddSample("/job_proxy/preemptable_cpu_x100", static_cast<i64>((HardLimit_ - SoftLimit_) * 100));
    }
}

void TCpuMonitor::DoCheck()
{
    if (!UpdateSmoothedValue()) {
        return;
    }
    UpdateVotes();

    auto decision = TryMakeDecision();
    if (decision) {
        LOG_DEBUG("Soft limit changed (OldValue: %v, NewValue: %v)", SoftLimit_, *decision);
        SoftLimit_ = *decision;
        if (Config_->EnableCpuReclaiming) {
            JobProxy_->SetCpuLimit(*decision);
        }
    }
};

bool TCpuMonitor::UpdateSmoothedValue()
{
    TDuration totalCpu;
    try {
        totalCpu = JobProxy_->GetSpentCpuTime();
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Failed to get cpu statistics");
        return false;
    }

    auto now = TInstant::Now();
    bool canCalcSmoothedUsage = LastCheckTime_.HasValue() && LastTotalCpu_.HasValue();
    if (canCalcSmoothedUsage) {
        auto deltaCpu = totalCpu - *LastTotalCpu_;
        auto cpuUsage = deltaCpu / (now - *LastCheckTime_);
        auto newSmoothedUsage = SmoothedUsage_.HasValue()
            ? Config_->SmoothingFactor * cpuUsage + (1 - Config_->SmoothingFactor) * *SmoothedUsage_
            : HardLimit_;
        LOG_DEBUG("Smoothed cpu usage updated (OldValue: %v, NewValue: %v)", *SmoothedUsage_, newSmoothedUsage);
        SmoothedUsage_ = newSmoothedUsage;
    }
    LastCheckTime_ = now;
    LastTotalCpu_ = totalCpu;
    return canCalcSmoothedUsage;
}

void TCpuMonitor::UpdateVotes()
{
    double ratio = *SmoothedUsage_ / SoftLimit_;

    if (ratio < Config_->RelativeLowerBound) {
        Votes_.emplace_back(EVote::Decrease);
    } else if (ratio > Config_->RelativeUpperBound) {
        Votes_.emplace_back(EVote::Increase);
    } else {
        Votes_.emplace_back(EVote::Keep);
    }
}

TNullable<double> TCpuMonitor::TryMakeDecision()
{
    TNullable<double> result;
    if (Votes_.size() >= Config_->VoteWindowSize) {
        auto voteSum = 0;
        for (const auto& vote : Votes_) {
            if (vote == EVote::Increase) {
                ++voteSum;
            } else if (vote == EVote::Decrease) {
                --voteSum;
            }
        }
        if (voteSum > Config_->VoteDecisionThreshold) {
            auto softLimit = std::min(SoftLimit_ * Config_->IncreaseCoefficient, HardLimit_);
            if (softLimit != SoftLimit_) {
                result = softLimit;
            }
            Votes_.clear();
        } else if (voteSum < -Config_->VoteDecisionThreshold) {
            auto softLimit = std::max(SoftLimit_ * Config_->DecreaseCoefficient, Config_->MinCpuLimit);
            if (softLimit != SoftLimit_) {
                result = softLimit;
            }
            Votes_.clear();
        } else {
            Votes_.pop_front();
        }
    }
    return result;
}

DEFINE_REFCOUNTED_TYPE(TCpuMonitor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
