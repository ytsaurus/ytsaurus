#pragma once

#include "public.h"
#include "serialize.h"

#include <yt/server/chunk_pools/public.h>

#include <yt/server/scheduler/job.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

struct TBriefJobStatistics
    : public TIntrinsicRefCounted
{
    TInstant Timestamp = TInstant::Zero();

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

    void Persist(const NPhoenix::TPersistenceContext& context);
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

// Performs statistics parsing and put it inside jobSummary.
void ParseStatistics(TJobSummary* jobSummary, const NYson::TYsonString& lastObservedStatisticsYson = NYson::TYsonString());

////////////////////////////////////////////////////////////////////////////////

struct TScheduleJobStatistics
    : public TIntrinsicRefCounted
    , public IPersistent
{
    void RecordJobResult(const TScheduleJobResult& scheduleJobResult);

    TEnumIndexedVector<int, EScheduleJobFailReason> Failed;
    TDuration Duration;
    i64 Count = 0;

    void Persist(const TPersistenceContext& context);
};

DEFINE_REFCOUNTED_TYPE(TScheduleJobStatistics)

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
