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
    EJobType Type = EJobType::SchedulerUnknown;

    std::optional<TInstant> FinishTime;
    NJobAgent::TTimeStatistics TimeStatistics;

    // NB: The Statistics field will be set inside the controller in ParseStatistics().
    std::optional<TStatistics> Statistics;
    NYson::TYsonString StatisticsYson;

    bool LogAndProfile = false;

    NJobTrackerClient::TReleaseJobFlags ReleaseFlags;

    TInstant LastStatusUpdateTime;
    bool JobExecutionCompleted = false;
};

struct TCompletedJobSummary
    : public TJobSummary
{
    TCompletedJobSummary() = default;
    explicit TCompletedJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event);
    explicit TCompletedJobSummary(NJobTrackerClient::NProto::TJobStatus* status);

    void Persist(const TPersistenceContext& context);

    bool Abandoned = false;
    EInterruptReason InterruptReason = EInterruptReason::None;

    // These fields are for controller's use only.
    std::vector<NChunkClient::TLegacyDataSlicePtr> UnreadInputDataSlices;
    std::vector<NChunkClient::TLegacyDataSlicePtr> ReadInputDataSlices;
    int SplitJobCount = 1;

    inline static constexpr EJobState ExpectedState = EJobState::Completed;
};

struct TAbortedJobSummary
    : public TJobSummary
{
    TAbortedJobSummary(TJobId id, EAbortReason abortReason);
    TAbortedJobSummary(const TJobSummary& other, EAbortReason abortReason);
    explicit TAbortedJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event);
    explicit TAbortedJobSummary(NJobTrackerClient::NProto::TJobStatus* status);

    EAbortReason AbortReason = EAbortReason::None;
    std::optional<NScheduler::TPreemptedFor> PreemptedFor;
    bool AbortedByScheduler{};

    inline static constexpr EJobState ExpectedState = EJobState::Aborted;
};

struct TFailedJobSummary
    : public TJobSummary
{
    explicit TFailedJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event);
    explicit TFailedJobSummary(NJobTrackerClient::NProto::TJobStatus* status);

    inline static constexpr EJobState ExpectedState = EJobState::Failed;
};

struct TRunningJobSummary
    : public TJobSummary
{
    explicit TRunningJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event);
    explicit TRunningJobSummary(NJobTrackerClient::NProto::TJobStatus* status);

    double Progress = 0;
    i64 StderrSize = 0;

    inline static constexpr EJobState ExpectedState = EJobState::Running;
};

std::unique_ptr<TJobSummary> ParseJobSummary(
    NJobTrackerClient::NProto::TJobStatus* const status,
    const NLogging::TLogger& Logger);

std::unique_ptr<TFailedJobSummary> MergeJobSummaries(
    std::unique_ptr<TFailedJobSummary> schedulerJobSummary,
    std::unique_ptr<TFailedJobSummary> nodeJobSummary);

std::unique_ptr<TAbortedJobSummary> MergeJobSummaries(
    std::unique_ptr<TAbortedJobSummary> schedulerJobSummary,
    std::unique_ptr<TAbortedJobSummary> nodeJobSummary);

std::unique_ptr<TCompletedJobSummary> MergeJobSummaries(
    std::unique_ptr<TCompletedJobSummary> schedulerJobSummary,
    std::unique_ptr<TCompletedJobSummary> nodeJobSummary);

std::unique_ptr<TJobSummary> MergeSchedulerAndNodeFinishedJobSummaries(
    std::unique_ptr<TJobSummary> schedulerJobSummary,
    std::unique_ptr<TJobSummary> nodeJobSummary);

template <class TJobSummaryType>
std::unique_ptr<TJobSummaryType> SummaryCast(std::unique_ptr<TJobSummary> jobSummary) noexcept
{
    YT_VERIFY(jobSummary->State == TJobSummaryType::ExpectedState);
    return std::unique_ptr<TJobSummaryType>{static_cast<TJobSummaryType*>(jobSummary.release())};
}

// COMPAT(pogorelov)
bool ExpectsJobInfoSeparately(const TJobSummary& jobSummary) noexcept;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
