#pragma once

#include "public.h"

#include <yt/yt/server/lib/controller_agent/persistence.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ELegacyLivePreviewMode,
    (ExplicitlyEnabled)
    (ExplicitlyDisabled)
    (DoNotCare)
    (NotSupported)
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IOperationController)
using IOperationControllerWeakPtr = TWeakPtr<IOperationController>;

DECLARE_REFCOUNTED_STRUCT(TSnapshotJob)

DECLARE_REFCOUNTED_CLASS(TSnapshotBuilder)
DECLARE_REFCOUNTED_CLASS(TSnapshotDownloader)

DECLARE_REFCOUNTED_CLASS(TChunkListPool)

DECLARE_REFCOUNTED_CLASS(TZombieOperationOrchids)

struct TLivePreviewTableBase;

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ControllerLogger;
extern const NLogging::TLogger ControllerAgentLogger;
extern const NLogging::TLogger ControllerEventLogger;
extern const NLogging::TLogger ControllerFeatureStructuredLogger;

extern const NProfiling::TProfiler ControllerAgentProfiler;

////////////////////////////////////////////////////////////////////////////////

using TOperationIdToControllerMap = THashMap<TOperationId, IOperationControllerPtr>;
using TOperationIdToWeakControllerMap = THashMap<TOperationId, IOperationControllerWeakPtr>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

