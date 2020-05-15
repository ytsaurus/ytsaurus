#pragma once

#include "public.h"

#include <yt/ytlib/controller_agent/public.h>

#include <yt/core/profiling/metrics_accumulator.h>

#include <yt/core/misc/phoenix.h>
#include <yt/core/misc/public.h>

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
    (PrepareRootFSTime)
    (ArtifactsDownloadTime)

    (TotalTimeCompleted)
    (TotalTimeAborted)

    (TotalTimeOperationCompleted)
    (TotalTimeOperationFailed)
    (TotalTimeOperationAborted)
);

DEFINE_ENUM(EAggregateType,
    (Sum)
    (Min)
    (Max)
);

struct TCustomJobMetricDescription
{
    TString StatisticsPath;
    TString ProfilingName;
    EAggregateType AggregateType = EAggregateType::Sum;
    std::optional<NJobTrackerClient::EJobState> JobStateFilter;

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
    using TValues = TEnumIndexedVector<EJobMetricName, i64>;
    using TCustomValues = THashMap<TCustomJobMetricDescription, i64>;
    DEFINE_BYREF_RW_PROPERTY(TValues, Values);
    DEFINE_BYREF_RW_PROPERTY(TCustomValues, CustomValues);

    static TJobMetrics FromJobStatistics(
        const TStatistics& statistics,
        NJobTrackerClient::EJobState jobState,
        const std::vector<TCustomJobMetricDescription>& customJobMetrics);

    bool IsEmpty() const;

    void Profile(
        NProfiling::TMetricsAccumulator& accumulator,
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

bool Dominates(const TJobMetrics& lhs, const TJobMetrics& rhs);

void ToProto(NControllerAgent::NProto::TJobMetrics* protoJobMetrics, const NScheduler::TJobMetrics& jobMetrics);
void FromProto(NScheduler::TJobMetrics* jobMetrics, const NControllerAgent::NProto::TJobMetrics& protoJobMetrics);

////////////////////////////////////////////////////////////////////////////////

struct TTreeTaggedJobMetrics
{
    TString TreeId;
    TJobMetrics Metrics;
};

using TOperationJobMetrics = std::vector<TTreeTaggedJobMetrics>;
using TOperationIdToOperationJobMetrics = THashMap<TOperationId, TOperationJobMetrics>;

void ToProto(
    NControllerAgent::NProto::TTreeTaggedJobMetrics* protoJobMetrics,
    const NScheduler::TTreeTaggedJobMetrics& jobMetrics);

void FromProto(
    NScheduler::TTreeTaggedJobMetrics* jobMetrics,
    const NControllerAgent::NProto::TTreeTaggedJobMetrics& protoJobMetrics);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
