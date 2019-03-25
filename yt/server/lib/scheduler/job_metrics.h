#pragma once

#include "public.h"

#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/ytlib/scheduler/public.h>

#include <yt/core/profiling/metrics_accumulator.h>

#include <yt/core/misc/phoenix.h>

#include <util/system/defaults.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EJobMetricName,
    // We use Io instead of IO in enum values since we want to have 'io'
    // in FormatEnum representation.
    (UserJobIoReads)
    (UserJobIoWrites)
    (UserJobIoTotal)
    (UserJobBytesRead)
    (UserJobBytesWritten)

    (AggregatedSmoothedCpuUsageX100)
    (AggregatedMaxCpuUsageX100)
    (AggregatedPreemptableCpuX100)
    (AggregatedPreemptedCpuX100)

    (TotalTime)
    (ExecTime)
    (PrepareTime)
    (ArtifactsDownloadTime)

    (TotalTimeCompleted)
    (TotalTimeAborted)
);

struct TCustomJobMetricDescription
{
    TString StatisticsPath;
    TString ProfilingName;

    void Persist(const TStreamPersistenceContext& context);
};

bool operator==(const TCustomJobMetricDescription& lhs, const TCustomJobMetricDescription& rhs);
bool operator<(const TCustomJobMetricDescription& lhs, const TCustomJobMetricDescription& rhs);

void Serialize(const TCustomJobMetricDescription& filter, NYson::IYsonConsumer* consumer);
void Deserialize(TCustomJobMetricDescription& filter, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NYT::NScheduler::TCustomJobMetricDescription>
{
    size_t operator()(const NYT::NScheduler::TCustomJobMetricDescription& jobMetricDescription) const;
};

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TJobMetrics
{
public:
    using TValues = TEnumIndexedVector<i64, EJobMetricName>;
    using TCustomValues = THashMap<TCustomJobMetricDescription, i64>;
    DEFINE_BYREF_RW_PROPERTY(TValues, Values);
    DEFINE_BYREF_RW_PROPERTY(TCustomValues, CustomValues);

    static TJobMetrics FromJobTrackerStatistics(
        const NJobTrackerClient::TStatistics& statistics,
        NJobTrackerClient::EJobState jobState,
        const std::vector<TCustomJobMetricDescription>& customJobMetrics);

    bool IsEmpty() const;

    void Profile(
        NProfiling::TMetricsAccumulator& collector,
        const TString& prefix,
        const NProfiling::TTagIdList& tagIds) const;

    void Persist(const NPhoenix::TPersistenceContext& context);

private:

    friend TJobMetrics& operator+=(TJobMetrics& lhs, const TJobMetrics& rhs);
    friend TJobMetrics& operator-=(TJobMetrics& lhs, const TJobMetrics& rhs);
    friend TJobMetrics operator+(const TJobMetrics& lhs, const TJobMetrics& rhs);
    friend TJobMetrics operator-(const TJobMetrics& lhs, const TJobMetrics& rhs);
};

////////////////////////////////////////////////////////////////////////////////

TJobMetrics& operator+=(TJobMetrics& lhs, const TJobMetrics& rhs);
TJobMetrics& operator-=(TJobMetrics& lhs, const TJobMetrics& rhs);
TJobMetrics operator+(const TJobMetrics& lhs, const TJobMetrics& rhs);
TJobMetrics operator-(const TJobMetrics& lhs, const TJobMetrics& rhs);

void ToProto(NScheduler::NProto::TJobMetrics* protoJobMetrics, const NScheduler::TJobMetrics& jobMetrics);
void FromProto(NScheduler::TJobMetrics* jobMetrics, const NScheduler::NProto::TJobMetrics& protoJobMetrics);

////////////////////////////////////////////////////////////////////////////////

struct TTreeTaggedJobMetrics
{
    TString TreeId;
    TJobMetrics Metrics;
};

using TOperationJobMetrics = std::vector<TTreeTaggedJobMetrics>;
using TOperationIdToOperationJobMetrics = THashMap<TOperationId, TOperationJobMetrics>;

void ToProto(
    NScheduler::NProto::TTreeTaggedJobMetrics* protoJobMetrics,
    const NScheduler::TTreeTaggedJobMetrics& jobMetrics);

void FromProto(
    NScheduler::TTreeTaggedJobMetrics* jobMetrics,
    const NScheduler::NProto::TTreeTaggedJobMetrics& protoJobMetrics);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
