#pragma once

#include "private.h"

#include <yt/yt/server/lib/chunk_pools/public.h>

#include <yt/yt/server/lib/controller_agent/persistence.h>

#include <yt/yt/server/lib/scheduler/public.h>

#include <yt/yt/core/misc/moving_average.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

struct TBriefJobStatistics
    : public TRefCounted
{
    TInstant Timestamp = TInstant::Zero();
    EJobPhase Phase;

    i64 ProcessedInputRowCount = 0;
    i64 ProcessedInputUncompressedDataSize = 0;
    i64 ProcessedInputDataWeight = 0;
    i64 ProcessedInputCompressedDataSize = 0;
    i64 ProcessedOutputRowCount = 0;
    i64 ProcessedOutputUncompressedDataSize = 0;
    i64 ProcessedOutputCompressedDataSize = 0;
    // Time is given in milliseconds.
    std::optional<i64> InputPipeIdleTime = std::nullopt;
    // Maximum across all output tables. This should work fine.
    std::optional<i64> OutputPipeIdleTime = std::nullopt;
    std::optional<i64> JobProxyCpuUsage = std::nullopt;

    void Persist(const TPersistenceContext& context);
};

DEFINE_REFCOUNTED_TYPE(TBriefJobStatistics)

void Serialize(const TBriefJobStatisticsPtr& briefJobStatistics, NYson::IYsonConsumer* consumer);

TString ToString(const TBriefJobStatisticsPtr& briefStatistics);

////////////////////////////////////////////////////////////////////////////////

TBriefJobStatisticsPtr BuildBriefStatistics(std::unique_ptr<TJobSummary> jobSummary);

// Returns true if job proxy wasn't stalling and false otherwise.
// This function is related to the suspicious jobs detection.
bool CheckJobActivity(
    const TBriefJobStatisticsPtr& lhs,
    const TBriefJobStatisticsPtr& rhs,
    const TSuspiciousJobsOptionsPtr& options,
    EJobType jobType);

//! Update joblet as a reaction on running or finished job summary.
//! - If job summary contains statistics, put them to joblet as job statistics.
//! - Recalculate joblet controller statistics from scratch.
//! - Update some auxiliary fields like FinishTime.
void UpdateJobletFromSummary(
    const TJobSummary& jobSummary,
    const TJobletPtr& joblet);

////////////////////////////////////////////////////////////////////////////////

class TScheduleJobStatistics
    : public TRefCounted
    , public IPersistent
{
public:
    //! Persistent statistics.
    using TScheduleJobFailReasonCounter = TEnumIndexedVector<EScheduleJobFailReason, int>;
    DEFINE_BYREF_RW_PROPERTY(TScheduleJobFailReasonCounter, Failed);

    DEFINE_BYVAL_RO_PROPERTY(TDuration, TotalDuration);
    DEFINE_BYVAL_RO_PROPERTY(i64, Count);

    //! Transient statistics.
    // TODO(eshcherbin): Use exponential moving average for O(1) memory?
    DEFINE_BYREF_RO_PROPERTY(TMovingAverage<TDuration>, SuccessfulDurationMovingAverage);

public:
    TScheduleJobStatistics() = default;
    explicit TScheduleJobStatistics(int movingAverageWindowSize);

    void RecordJobResult(const NScheduler::TControllerScheduleJobResult& scheduleJobResult);

    void SetMovingAverageWindowSize(int movingAverageWindowSize);

    void Persist(const TPersistenceContext& context);
};

DEFINE_REFCOUNTED_TYPE(TScheduleJobStatistics)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
