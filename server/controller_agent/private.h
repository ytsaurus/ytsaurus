#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

extern const TString IntermediatePath;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IOperationController)
using IOperationControllerWeakPtr = TWeakPtr<IOperationController>;

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

DECLARE_REFCOUNTED_CLASS(TAutoMergeTask)

DECLARE_REFCOUNTED_STRUCT(ITaskHost)

DECLARE_REFCOUNTED_STRUCT(TInputTable)
DECLARE_REFCOUNTED_STRUCT(TOutputTable)
DECLARE_REFCOUNTED_STRUCT(TIntermediateTable)

struct IJobSplitter;

struct TLivePreviewTableBase;

class TAutoMergeDirector;
struct TJobNodeDescriptor;

////////////////////////////////////////////////////////////////////////////////

extern const double ApproximateSizesBoostFactor;
extern const double JobSizeBoostFactor;

extern const TDuration PrepareYieldPeriod;

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ControllerLogger;
extern const NLogging::TLogger ControllerAgentLogger;

extern const NProfiling::TProfiler ControllerAgentProfiler;

////////////////////////////////////////////////////////////////////////////////

using TOperationIdToControllerMap = THashMap<TOperationId, IOperationControllerPtr>;
using TOperationIdToWeakControllerMap = THashMap<TOperationId, IOperationControllerWeakPtr>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

