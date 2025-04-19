#pragma once

#include "public.h"

#include <yt/yt/ytlib/controller_agent/persistence.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/tracing/trace_context.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ELegacyLivePreviewMode,
    (ExplicitlyEnabled)
    (ExplicitlyDisabled)
    (DoNotCare)
    (NotSupported)
);

////////////////////////////////////////////////////////////////////////////////

constexpr auto OperationIdTag = "operation_id";

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IOperationController)
using IOperationControllerWeakPtr = TWeakPtr<IOperationController>;

DECLARE_REFCOUNTED_STRUCT(IPartitioningParametersEvaluator)

DECLARE_REFCOUNTED_STRUCT(TSnapshotJob)

DECLARE_REFCOUNTED_CLASS(TSnapshotBuilder)
DECLARE_REFCOUNTED_CLASS(TSnapshotDownloader)

DECLARE_REFCOUNTED_CLASS(TChunkListPool)

DECLARE_REFCOUNTED_CLASS(TZombieOperationOrchids)

DECLARE_REFCOUNTED_CLASS(TJobProfiler)

DECLARE_REFCOUNTED_STRUCT(TJobTrackerTestingOptions)
DECLARE_REFCOUNTED_CLASS(TJobTracker)
DECLARE_REFCOUNTED_CLASS(TJobTrackerOperationHandler)
DECLARE_REFCOUNTED_STRUCT(TJobTrackerConfig)
DECLARE_REFCOUNTED_STRUCT(TGangManagerConfig)

DECLARE_REFCOUNTED_STRUCT(TDockerRegistryConfig)

DECLARE_REFCOUNTED_STRUCT(TDisallowRemoteOperationsConfig)

struct TJobStartInfo
{
    TJobId JobId;
    TSharedRef JobSpecBlob;
};

struct TStartedJobInfo
{
    TJobId JobId;
};

struct TStartedAllocationInfo
{
    TAllocationId AllocationId;
    std::string NodeAddress;
    std::optional<TStartedJobInfo> StartedJobInfo;
};

struct TJobMonitoringDescriptor
{
    TIncarnationId IncarnationId;
    int Index = 0;

    void Persist(const TPersistenceContext& context);

    auto operator<=>(const TJobMonitoringDescriptor& other) const = default;
};

inline const TJobMonitoringDescriptor NullMonitoringDescriptor{
    .IncarnationId = TIncarnationId(),
    .Index = -1,
};

void FormatValue(TStringBuilderBase* builder, const TJobMonitoringDescriptor& descriptor, TStringBuf /*spec*/);

struct TLivePreviewTableBase;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, ControllerLogger, "Controller");
YT_DEFINE_GLOBAL(const NLogging::TLogger, ControllerAgentLogger, "ControllerAgent");
YT_DEFINE_GLOBAL(const NLogging::TLogger, ControllerEventLogger, NLogging::TLogger("ControllerEventLog").WithEssential());
YT_DEFINE_GLOBAL(const NLogging::TLogger, ControllerFeatureStructuredLogger, NLogging::TLogger("ControllerFeatureStructuredLog").WithEssential());

YT_DEFINE_GLOBAL(const NProfiling::TProfiler, ControllerAgentProfiler, "/controller_agent");

////////////////////////////////////////////////////////////////////////////////

using TOperationIdToControllerMap = THashMap<TOperationId, IOperationControllerPtr>;
using TOperationIdToWeakControllerMap = THashMap<TOperationId, IOperationControllerWeakPtr>;

////////////////////////////////////////////////////////////////////////////////

//! Creates a child trace context for operation and set allocation tags to it.
//! Returns TraceContextGuard with the trace context.

NTracing::TTraceContextGuard CreateOperationTraceContextGuard(
    TString spanName,
    TOperationId operationId);

////////////////////////////////////////////////////////////////////////////////

struct TCompositePendingJobCount
{
    int DefaultCount = 0;
    THashMap<TString, int> CountByPoolTree = {};

    int GetJobCountFor(const TString& tree) const;
    bool IsZero() const;

    void Persist(const TStreamPersistenceContext& context);
};

void Serialize(const TCompositePendingJobCount& jobCount, NYson::IYsonConsumer* consumer);

void FormatValue(TStringBuilderBase* builder, const TCompositePendingJobCount& jobCount, TStringBuf /*format*/);

bool operator == (const TCompositePendingJobCount& lhs, const TCompositePendingJobCount& rhs);

TCompositePendingJobCount operator + (const TCompositePendingJobCount& lhs, const TCompositePendingJobCount& rhs);
TCompositePendingJobCount operator - (const TCompositePendingJobCount& lhs, const TCompositePendingJobCount& rhs);
TCompositePendingJobCount operator - (const TCompositePendingJobCount& count);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

template <>
struct THash<NYT::NControllerAgent::TJobMonitoringDescriptor>
{
    size_t operator()(const NYT::NControllerAgent::TJobMonitoringDescriptor& descriptor) const;
};
