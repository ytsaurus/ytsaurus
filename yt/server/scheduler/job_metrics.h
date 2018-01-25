#pragma once

#include "public.h"

#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/ytlib/scheduler/public.h>

#include <yt/core/profiling/public.h>

#include <yt/core/misc/phoenix.h>

#include <util/system/defaults.h>

namespace NYT {
namespace NScheduler {

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
    DEFINE_BYVAL_RW_PROPERTY(i64, TimeCompleted);
    DEFINE_BYVAL_RW_PROPERTY(i64, TimeAborted);

public:
    static TJobMetrics FromJobTrackerStatistics(
        const NJobTrackerClient::TStatistics& statistics,
        NJobTrackerClient::EJobState jobState);

    bool IsEmpty() const;

    void SendToProfiler(
        const NProfiling::TProfiler& profiler,
        const TString& prefix,
        const NProfiling::TTagIdList& tagIds) const;

    void Persist(const NPhoenix::TPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

TJobMetrics& operator+=(TJobMetrics& lhs, const TJobMetrics& rhs);
TJobMetrics& operator-=(TJobMetrics& lhs, const TJobMetrics& rhs);
TJobMetrics operator+(const TJobMetrics& lhs, const TJobMetrics& rhs);
TJobMetrics operator-(const TJobMetrics& lhs, const TJobMetrics& rhs);

namespace NProto {

void ToProto(NScheduler::NProto::TJobMetrics* protoJobMetrics, const NScheduler::TJobMetrics& jobMetrics);
void FromProto(NScheduler::TJobMetrics* jobMetrics, const NScheduler::NProto::TJobMetrics& protoJobMetrics);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

struct TTreeTaggedJobMetrics
{
    TString TreeId;
    TJobMetrics Metrics;
};

namespace NProto {

void ToProto(
    NScheduler::NProto::TTreeTaggedJobMetrics* protoJobMetrics,
    const NScheduler::TTreeTaggedJobMetrics& jobMetrics);

void FromProto(
    NScheduler::TTreeTaggedJobMetrics* jobMetrics,
    const NScheduler::NProto::TTreeTaggedJobMetrics& protoJobMetrics);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

struct TOperationJobMetrics
{
    TOperationId OperationId;
    std::vector<TTreeTaggedJobMetrics> Metrics;
};

namespace NProto {

void ToProto(
    NScheduler::NProto::TOperationJobMetrics* protoOperationJobMetrics,
    const NScheduler::TOperationJobMetrics& operationJobMetrics);

void FromProto(
    NScheduler::TOperationJobMetrics* operationJobMetrics,
    const NScheduler::NProto::TOperationJobMetrics& protoOperationJobMetrics);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
