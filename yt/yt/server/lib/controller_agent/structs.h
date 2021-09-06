#pragma once

#include "persistence.h"
#include "helpers.h"

#include <yt/yt/server/lib/scheduler/public.h>
#include <yt/yt/server/lib/scheduler/structs.h>

#include <yt/yt/server/lib/job_agent/job_report.h>

#include <yt/yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/misc/phoenix.h>
#include <yt/yt/core/misc/statistics.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

// NB: This particular summary does not inherit from TJobSummary.
struct TStartedJobSummary
{
    explicit TStartedJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event);

    TJobId Id;
    TInstant StartTime;
};

struct TJobSummary
{
    TJobSummary() = default;
    TJobSummary(TJobId id, EJobState state);
    explicit TJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event);
    explicit TJobSummary(NJobTrackerClient::NProto::TJobStatus* status);

    virtual ~TJobSummary() = default;

    void Persist(const TPersistenceContext& context);

    NJobTrackerClient::NProto::TJobResult Result;
    TJobId Id;
    EJobState State = EJobState::None;
    EJobPhase Phase = EJobPhase::Missing;

    std::optional<TInstant> FinishTime;
    NJobAgent::TTimeStatistics TimeStatistics;

    // NB: The Statistics field will be set inside the controller in ParseStatistics().
    std::optional<TStatistics> Statistics;
    NYson::TYsonString StatisticsYson;

    bool LogAndProfile = false;

    NJobTrackerClient::TReleaseJobFlags ReleaseFlags;

    TInstant LastStatusUpdateTime;
};

struct TCompletedJobSummary
    : public TJobSummary
{
    TCompletedJobSummary() = default;
    explicit TCompletedJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event);

    void Persist(const TPersistenceContext& context);

    bool Abandoned = false;
    EInterruptReason InterruptReason = EInterruptReason::None;

    // These fields are for controller's use only.
    std::vector<NChunkClient::TLegacyDataSlicePtr> UnreadInputDataSlices;
    std::vector<NChunkClient::TLegacyDataSlicePtr> ReadInputDataSlices;
    int SplitJobCount = 1;
};

struct TAbortedJobSummary
    : public TJobSummary
{
    TAbortedJobSummary(TJobId id, EAbortReason abortReason);
    TAbortedJobSummary(const TJobSummary& other, EAbortReason abortReason);
    explicit TAbortedJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event);

    EAbortReason AbortReason = EAbortReason::None;
    std::optional<NScheduler::TPreemptedFor> PreemptedFor;
};

struct TRunningJobSummary
    : public TJobSummary
{
    explicit TRunningJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event);
    explicit TRunningJobSummary(NJobTrackerClient::NProto::TJobStatus* status);

    double Progress = 0;
    i64 StderrSize = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
