#pragma once

#include "public.h"

#include <yt/server/chunk_pools/public.h>

#include <yt/server/scheduler/job.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

struct IJobSplitter
{
    virtual ~IJobSplitter() = default;

    virtual void OnJobStarted(
        const TJobId& jobId,
        const NChunkPools::TChunkStripeListPtr& inputStripeList) = 0;
    virtual void OnJobRunning(const NScheduler::TJobSummary& summary) = 0;
    virtual void OnJobFailed(const NScheduler::TFailedJobSummary& summary) = 0;
    virtual void OnJobAborted(const NScheduler::TAbortedJobSummary& summary) = 0;
    virtual void OnJobCompleted(const NScheduler::TCompletedJobSummary& summary) = 0;
    virtual int EstimateJobCount(
        const NScheduler::TCompletedJobSummary& summary,
        i64 unreadRowCount) const = 0;
    virtual bool IsJobSplittable(const TJobId& jobId) const = 0;
    virtual void BuildJobSplitterInfo(NYson::IYsonConsumer* consumer) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IJobSplitter> CreateJobSplitter(
    const NScheduler::TJobSplitterConfigPtr& config,
    const TOperationId& operationId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

