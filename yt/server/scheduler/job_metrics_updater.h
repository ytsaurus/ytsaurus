#pragma once

#include "job_metrics.h"

#include <yt/ytlib/job_tracker_client/public.h>

#include <util/datetime/base.h>

namespace NYT {
namespace NScheduler {

struct IOperationHost;

////////////////////////////////////////////////////////////////////

// TJobMetricsUpdater is responsible for computing metrics deltas
// and sending them to scheduler tree
class TJobMetricsUpdater
{
public:
    TJobMetricsUpdater(
        IOperationHost* host,
        const NJobTrackerClient::TOperationId& operationId,
        TDuration batchInterval);

    void Update(TInstant metricsTs, const TJobMetrics& jobMetrics);
    void Flush();

private:
    IOperationHost* const Host_;
    const NJobTrackerClient::TOperationId OperationId_;
    const TDuration BatchInterval_;

    TJobMetrics SentJobMetrics_;
    TInstant LastSeenTimestamp_;
    // Metrics that are not sent yet
    TNullable<TJobMetrics> LocalJobMetrics_;
    TInstant NextFlush_;
};

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
