#pragma once

#include "public.h"

#include "profiler.h"

#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/ytlib/scheduler/public.h>

#include <yt/core/profiling/public.h>

#include <yt/core/misc/phoenix.h>

#include <util/system/defaults.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TJobMetrics
{
    friend TJobMetrics& operator+=(TJobMetrics& lhs, const TJobMetrics& rhs);
    friend TJobMetrics& operator-=(TJobMetrics& lhs, const TJobMetrics& rhs);
    friend TJobMetrics operator+(const TJobMetrics& lhs, const TJobMetrics& rhs);
    friend TJobMetrics operator-(const TJobMetrics& lhs, const TJobMetrics& rhs);

public:
    DEFINE_BYVAL_RW_PROPERTY(i64, DiskReads);
    DEFINE_BYVAL_RW_PROPERTY(i64, DiskWrites);

    // TODO(ignat): make separate tag for job state.
    DEFINE_BYVAL_RW_PROPERTY(i64, TimeCompleted);
    DEFINE_BYVAL_RW_PROPERTY(i64, TimeAborted);

    DEFINE_BYVAL_RW_PROPERTY(i64, AggregatedSmoothedCpuUsage);
    DEFINE_BYVAL_RW_PROPERTY(i64, AggregatedMaxCpuUsage);
    DEFINE_BYVAL_RW_PROPERTY(i64, AggregatedPreemptableCpu);

    DEFINE_BYVAL_RW_PROPERTY(i64, TotalTime);
    DEFINE_BYVAL_RW_PROPERTY(i64, ExecTime);
    DEFINE_BYVAL_RW_PROPERTY(i64, PrepareTime);
    DEFINE_BYVAL_RW_PROPERTY(i64, ArtifactsDownloadTime);

public:
    static TJobMetrics FromJobTrackerStatistics(
        const NJobTrackerClient::TStatistics& statistics,
        NJobTrackerClient::EJobState jobState);

    bool IsEmpty() const;

    void Profile(
        TProfileCollector& collector,
        const TString& prefix,
        const NProfiling::TTagIdList& tagIds) const;

    void Persist(const NPhoenix::TPersistenceContext& context);
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
