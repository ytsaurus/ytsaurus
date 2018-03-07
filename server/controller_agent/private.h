#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

extern const TString IntermediatePath;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TSnapshotJob)

DECLARE_REFCOUNTED_CLASS(TSnapshotBuilder)
DECLARE_REFCOUNTED_CLASS(TSnapshotDownloader)

class TOperationControllerBase;

DECLARE_REFCOUNTED_CLASS(TChunkListPool)

DECLARE_REFCOUNTED_STRUCT(TFinishedJobInfo)
DECLARE_REFCOUNTED_STRUCT(TJobInfo)
DECLARE_REFCOUNTED_CLASS(TJoblet)
DECLARE_REFCOUNTED_STRUCT(TCompletedJob)

DECLARE_REFCOUNTED_CLASS(TTask)
DECLARE_REFCOUNTED_STRUCT(TTaskGroup)

DECLARE_REFCOUNTED_CLASS(TAutoMergeTask);

DECLARE_REFCOUNTED_STRUCT(ITaskHost)

struct IJobSplitter;

struct TLivePreviewTableBase;
struct TInputTable;
struct TOutputTable;

class TAutoMergeDirector;

////////////////////////////////////////////////////////////////////////////////

extern const double ApproximateSizesBoostFactor;
extern const double JobSizeBoostFactor;

extern const TDuration PrepareYieldPeriod;

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ControllerLogger;
extern const NLogging::TLogger ControllerAgentLogger;

extern const NProfiling::TProfiler ControllerAgentProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

