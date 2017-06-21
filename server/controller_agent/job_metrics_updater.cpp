#include "job_metrics_updater.h"

#include "operation_controller.h"


namespace NYT {
namespace NControllerAgent {

using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

TJobMetricsUpdater::TJobMetricsUpdater(
    IOperationHost* host,
    const NJobTrackerClient::TOperationId& operationId,
    TDuration batchInterval)
    : Host_(host)
    , OperationId_(operationId)
    , BatchInterval_(batchInterval)
{ }

void TJobMetricsUpdater::Update(TInstant jobMetricsTime, const TJobMetrics& jobMetrics)
{
    if (LastSeenTimestamp_ >= jobMetricsTime) {
        return;
    }
    LastSeenTimestamp_ = jobMetricsTime;
    LocalJobMetrics_ = jobMetrics;
    if (NextFlush_ == TInstant()) {
        NextFlush_ = jobMetricsTime + BatchInterval_;
    }
    if (jobMetricsTime >= NextFlush_) {
        Flush();
    }
};

void TJobMetricsUpdater::Flush()
{
    if (!LocalJobMetrics_) {
        return;
    }
    const auto delta = *LocalJobMetrics_ - SentJobMetrics_;
    Host_->SendJobMetricsToStrategy(OperationId_, delta);
    SentJobMetrics_ = *LocalJobMetrics_;
    LocalJobMetrics_.Reset();
    NextFlush_ = TInstant();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
