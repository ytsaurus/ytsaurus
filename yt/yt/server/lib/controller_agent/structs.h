#pragma once

#include "helpers.h"

#include <yt/yt/server/lib/scheduler/public.h>
#include <yt/yt/server/lib/scheduler/structs.h>
#include <yt/yt/server/lib/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/yt/server/lib/job_agent/structs.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/controller_agent/persistence.h>

#include <yt/yt/core/misc/statistics.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): does this need to belong to server/lib?
// TODO(max42): make this structure non-copyable.
struct TJobSummary
{
    TJobSummary() = default;
    TJobSummary(TJobId id, EJobState state);
    explicit TJobSummary(NProto::TJobStatus* status);

    virtual ~TJobSummary() = default;

    //! Crashes if job result is not combined yet.
    NProto::TJobResult& GetJobResult();
    const NProto::TJobResult& GetJobResult() const;

    //! Crashes if job result is not combined yet or it misses a scheduler job result extension.
    NControllerAgent::NProto::TJobResultExt& GetJobResultExt();
    const NControllerAgent::NProto::TJobResultExt& GetJobResultExt() const;

    //! Crashes if job result is not combined yet.
    const TError& GetError() const;

    //! Crashes if job result is not combined yet, and returns nullptr if scheduler job
    //! result extension is missing.
    const NControllerAgent::NProto::TJobResultExt* FindJobResultExt() const;

    // COMPAT(max42): remove this when data statistics are always sent separately from the rest of statistics.
    void FillDataStatisticsFromStatistics();

    std::optional<int> GetExitCode() const;

    // NB: may be nullopt or may miss scheduler job result extension while job
    // result is being combined from scheduler and node parts.
    // Prefer using GetJobResult() and GetJobResult() helpers.
    std::optional<NProto::TJobResult> Result;
    std::optional<TError> Error;

    TJobId Id;
    EJobState State = EJobState::None;
    EJobPhase Phase = EJobPhase::Missing;

    EInterruptionReason InterruptionReason = EInterruptionReason::None;

    std::optional<TInstant> FinishTime;
    NJobAgent::TTimeStatistics TimeStatistics;
    TInstant StartTime;

    //! Statistics produced by job and node. May be absent for running job events,
    //! always present for aborted/failed/completed job summaries.
    std::shared_ptr<const TStatistics> Statistics;

    //! Total input data statistics. May be absent for running job summary.
    std::optional<NChunkClient::NProto::TDataStatistics> TotalInputDataStatistics;
    //! Per-table output data statistics. May be absent for running job summary or for abandoned completed job summary.
    std::optional<std::vector<NChunkClient::NProto::TDataStatistics>> OutputDataStatistics;
    //! Total output data statistics. May be absent for running job summary or for abandoned completed job summary.
    std::optional<NChunkClient::NProto::TDataStatistics> TotalOutputDataStatistics;

    TReleaseJobFlags ReleaseFlags;

    TInstant StatusTimestamp;
    bool JobExecutionCompleted = false;
    bool FinishedOnNode = false;

    PHOENIX_DECLARE_TYPE(TJobSummary, 0x583efb2c);
};

struct TCompletedJobSummary
    : public TJobSummary
{
    TCompletedJobSummary() = default;
    explicit TCompletedJobSummary(NProto::TJobStatus* status);

    bool Abandoned = false;

    // These fields are for controller's use only.
    std::vector<NChunkClient::TLegacyDataSlicePtr> UnreadInputDataSlices;
    std::vector<NChunkClient::TLegacyDataSlicePtr> ReadInputDataSlices;
    int SplitJobCount = 1;

    inline static constexpr EJobState ExpectedState = EJobState::Completed;

    PHOENIX_DECLARE_TYPE(TCompletedJobSummary, 0x78811781);
};

std::unique_ptr<TCompletedJobSummary> CreateAbandonedJobSummary(TJobId jobId);

struct TAbortedJobSummary
    : public TJobSummary
{
    TAbortedJobSummary(TJobId id, EAbortReason abortReason);
    TAbortedJobSummary(const TJobSummary& other, EAbortReason abortReason);
    explicit TAbortedJobSummary(NProto::TJobStatus* status, const NLogging::TLogger& Logger);

    EAbortReason AbortReason = EAbortReason::None;

    std::optional<NScheduler::TPreemptedFor> PreemptedFor;

    bool Scheduled = true;

    inline static constexpr EJobState ExpectedState = EJobState::Aborted;
};

std::unique_ptr<TAbortedJobSummary> CreateAbortedJobSummary(
    TJobId jobId,
    TAbortedAllocationSummary&& eventSummary);

std::unique_ptr<TAbortedJobSummary> CreateAbortedJobSummary(
    TJobId jobId,
    TFinishedAllocationSummary&& eventSummary);

struct TFailedJobSummary
    : public TJobSummary
{
    explicit TFailedJobSummary(NProto::TJobStatus* status);

    inline static constexpr EJobState ExpectedState = EJobState::Failed;
};

struct TWaitingJobSummary
    : public TJobSummary
{
    explicit TWaitingJobSummary(NProto::TJobStatus* status);

    inline static constexpr EJobState ExpectedState = EJobState::Waiting;
};

struct TRunningJobSummary
    : public TJobSummary
{
    explicit TRunningJobSummary(NProto::TJobStatus* status);

    double Progress = 0;
    i64 StderrSize = 0;

    inline static constexpr EJobState ExpectedState = EJobState::Running;
};

struct TAbortedAllocationSummary
{
    TOperationId OperationId;
    TAllocationId Id;
    TInstant FinishTime;
    EAbortReason AbortReason;
    TError Error;
    bool Scheduled;
};

void ToProto(NScheduler::NProto::TSchedulerToAgentAbortedAllocationEvent* proto, const TAbortedAllocationSummary& summary);
void FromProto(TAbortedAllocationSummary* summary, const NScheduler::NProto::TSchedulerToAgentAbortedAllocationEvent* protoEvent);
void FormatValue(
    TStringBuilderBase* builder,
    const TAbortedAllocationSummary& abortedAllocationSummary,
    TStringBuf spec);

struct TFinishedAllocationSummary
{
    TOperationId OperationId;
    TAllocationId Id;
    TInstant FinishTime;
};

void ToProto(NScheduler::NProto::TSchedulerToAgentFinishedAllocationEvent* proto, const TFinishedAllocationSummary& summary);
void FromProto(TFinishedAllocationSummary* summary, const NScheduler::NProto::TSchedulerToAgentFinishedAllocationEvent* protoEvent);
void FormatValue(
    TStringBuilderBase* builder,
    const TFinishedAllocationSummary& finishedAllocationSummary,
    TStringBuf spec);

struct TSchedulerToAgentAllocationEvent
{
    std::variant<TFinishedAllocationSummary, TAbortedAllocationSummary> EventSummary;
};

void ToProto(NScheduler::NProto::TSchedulerToAgentAllocationEvent* proto, const TSchedulerToAgentAllocationEvent& event);
void FromProto(TSchedulerToAgentAllocationEvent* event, const NScheduler::NProto::TSchedulerToAgentAllocationEvent* protoEvent);
void FormatValue(
    TStringBuilderBase* builder,
    const TSchedulerToAgentAllocationEvent& allocationEvent,
    TStringBuf spec);

std::unique_ptr<TJobSummary> ParseJobSummary(
    NProto::TJobStatus* const status,
    const NLogging::TLogger& Logger);

template <class TJobSummaryType>
std::unique_ptr<TJobSummaryType> SummaryCast(std::unique_ptr<TJobSummary> jobSummary) noexcept
{
    YT_VERIFY(jobSummary->State == TJobSummaryType::ExpectedState);
    return std::unique_ptr<TJobSummaryType>{static_cast<TJobSummaryType*>(jobSummary.release())};
}

template <class TJobSummaryType>
const TJobSummaryType& SummaryCast(const TJobSummary& jobSummary) noexcept
{
    YT_VERIFY(jobSummary.State == TJobSummaryType::ExpectedState);
    return static_cast<const TJobSummaryType&>(jobSummary);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
