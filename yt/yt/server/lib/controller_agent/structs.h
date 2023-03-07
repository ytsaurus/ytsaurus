#pragma once

#include "public.h"

#include "helpers.h"

#include <yt/server/lib/scheduler/public.h>
#include <yt/server/lib/scheduler/structs.h>

#include <yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/core/misc/phoenix.h>
#include <yt/core/misc/statistics.h>

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
    virtual ~TJobSummary() = default;

    void Persist(const NPhoenix::TPersistenceContext& context);

    NJobTrackerClient::NProto::TJobResult Result;
    TJobId Id;
    EJobState State = EJobState::None;
    EJobPhase Phase = EJobPhase::Missing;

    std::optional<TInstant> FinishTime;
    std::optional<TDuration> PrepareDuration;
    std::optional<TDuration> DownloadDuration;
    std::optional<TDuration> ExecDuration;
    std::optional<TDuration> PrepareRootFSDuration;

    // NB: The Statistics field will be set inside the controller in ParseStatistics().
    std::optional<TStatistics> Statistics;
    NYson::TYsonString StatisticsYson;

    bool LogAndProfile = false;

    NJobTrackerClient::TReleaseJobFlags ReleaseFlags;
};

struct TCompletedJobSummary
    : public TJobSummary
{
    TCompletedJobSummary() = default;
    explicit TCompletedJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event);

    void Persist(const NPhoenix::TPersistenceContext& context);

    bool Abandoned = false;
    EInterruptReason InterruptReason = EInterruptReason::None;

    // These fields are for controller's use only.
    std::vector<NChunkClient::TInputDataSlicePtr> UnreadInputDataSlices;
    std::vector<NChunkClient::TInputDataSlicePtr> ReadInputDataSlices;
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

    double Progress = 0;
    i64 StderrSize = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
