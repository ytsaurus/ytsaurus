#pragma once

#include "public.h"
#include "job.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct IJobSplitter
{
    virtual ~IJobSplitter() = default;

    virtual void OnJobStarted(
        const TJobId& jobId,
        const TChunkStripeListPtr& inputStripeList) = 0;
    virtual void OnJobRunning(const TJobSummary& summary) = 0;
    virtual void OnJobFailed(const TFailedJobSummary& summary) = 0;
    virtual void OnJobAborted(const TAbortedJobSummary& summary) = 0;
    virtual void OnJobCompleted(const TCompletedJobSummary& summary) = 0;
    virtual int EstimateJobCount(
        const TCompletedJobSummary& summary,
        i64 unreadRowCount) const = 0;
    virtual bool IsJobSplittable(const TJobId& jobId) const = 0;
    virtual void BuildJobSplitterInfo(NYson::IYsonConsumer* consumer) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IJobSplitter> CreateJobSplitter(
    const TJobSplitterConfigPtr& config,
    const TOperationId& operationId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

