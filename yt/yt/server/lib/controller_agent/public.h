#pragma once

#include <yt/yt/server/lib/scheduler/public.h>

#include <yt/yt/ytlib/controller_agent/public.h>

#include <yt/yt/ytlib/scheduler/public.h>

#include <yt/yt/core/phoenix/type_decl.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

using NScheduler::TAllocationId;
using NScheduler::TExecNodeDescriptorMap;
using NScheduler::TRefCountedExecNodeDescriptorMapPtr;
using NScheduler::TIncarnationId;
using NScheduler::TAgentId;
using NScheduler::EOperationAlertType;

////////////////////////////////////////////////////////////////////////////////

struct TJobSummary;
struct TCompletedJobSummary;
struct TAbortedJobSummary;
struct TFailedJobSummary;
struct TRunningJobSummary;
struct TAbortedAllocationSummary;
struct TFinishedAllocationSummary;
struct TIncarnationSwitchInfo;
struct TIncarnationSwitchData;

DECLARE_REFCOUNTED_CLASS(TLegacyProgressCounter)
DECLARE_REFCOUNTED_CLASS(TProgressCounter)
DECLARE_REFCOUNTED_STRUCT(IJobSizeConstraints)

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TJobSpec;
class TJobStatus;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration PrepareYieldPeriod = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

extern const NStatisticPath::TStatisticPath InputRowCountPath;
extern const NStatisticPath::TStatisticPath InputUncompressedDataSizePath;
extern const NStatisticPath::TStatisticPath InputCompressedDataSizePath;
extern const NStatisticPath::TStatisticPath InputDataWeightPath;
extern const NStatisticPath::TStatisticPath InputPipeIdleTimePath;
extern const NStatisticPath::TStatisticPath JobProxyCpuUsagePath;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EJobCompetitionType,
    (Speculative)
    (Probing)
    (Experiment)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
