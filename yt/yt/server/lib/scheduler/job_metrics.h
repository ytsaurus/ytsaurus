#pragma once

#include "public.h"

#include <yt/yt/server/lib/job_agent/public.h>

#include <yt/yt/ytlib/controller_agent/public.h>

#include <yt/yt/core/misc/phoenix.h>
#include <yt/yt/core/misc/public.h>

#include <yt/yt/library/profiling/producer.h>

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
    (AggregatedPreemptibleCpuX100)
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

    (MainResourceConsumptionOperationCompleted)
    (MainResourceConsumptionOperationFailed)
    (MainResourceConsumptionOperationAborted)
);

DEFINE_ENUM(ESummaryValueType,
    (Sum)
    (Min)
    (Max)
    (Last)
);

struct TCustomJobMetricDescription
{
    TString StatisticsPath;
    TString ProfilingName;
    ESummaryValueType SummaryValueType = ESummaryValueType::Sum;
    std::optional<NJobTrackerClient::EJobState> JobStateFilter = {};

    void Persist(const TStreamPersistenceContext& context);
};

bool operator==(const TCustomJobMetricDescription& lhs, const TCustomJobMetricDescription& rhs);
bool operator<(const TCustomJobMetricDescription& lhs, const TCustomJobMetricDescription& rhs);

void Serialize(const TCustomJobMetricDescription& filter, NYson::IYsonConsumer* consumer);
void Deserialize(TCustomJobMetricDescription& filter, NYTree::INodePtr node);
void Deserialize(TCustomJobMetricDescription& filter, NYson::TYsonPullParserCursor* cursor);

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
    using TValues = TEnumIndexedArray<EJobMetricName, i64>;
    using TCustomValues = THashMap<TCustomJobMetricDescription, i64>;
    DEFINE_BYREF_RW_PROPERTY(TValues, Values);
    DEFINE_BYREF_RW_PROPERTY(TCustomValues, CustomValues);

    static TJobMetrics FromJobStatistics(
        const TStatistics& jobStatistics,
        const TStatistics& controllerStatistics,
        const NJobAgent::TTimeStatistics& timeStatistics,
        NJobTrackerClient::EJobState jobState,
        const std::vector<TCustomJobMetricDescription>& customJobMetrics,
        bool considerNonMonotonicMetrics);

    bool IsEmpty() const;

    void Profile(NProfiling::ISensorWriter* writer) const;

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
TJobMetrics Max(const TJobMetrics& lhs, const TJobMetrics& rhs);

void ToProto(NControllerAgent::NProto::TJobMetrics* protoJobMetrics, const NScheduler::TJobMetrics& jobMetrics);
void FromProto(NScheduler::TJobMetrics* jobMetrics, const NControllerAgent::NProto::TJobMetrics& protoJobMetrics);

void Serialize(const TJobMetrics& jobMetrics, NYson::IYsonConsumer* consumer);

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
