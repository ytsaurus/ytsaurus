#pragma once

#include <yt/yt/server/lib/scheduler/public.h>

#include <yt/yt/ytlib/controller_agent/public.h>

#include <yt/yt/ytlib/scheduler/public.h>

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
struct TStartedJobSummary;
struct TCompletedJobSummary;
struct TAbortedJobSummary;
struct TFailedJobSummary;
struct TRunningJobSummary;
struct TAbortedBySchedulerJobSummary;
struct TFinishedJobSummary;

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

extern const TString InputRowCountPath;
extern const TString InputUncompressedDataSizePath;
extern const TString InputCompressedDataSizePath;
extern const TString InputDataWeightPath;
extern const TString InputPipeIdleTimePath;
extern const TString JobProxyCpuUsagePath;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EJobCompetitionType,
    (Speculative)
    (Probing)
    (Experiment)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
