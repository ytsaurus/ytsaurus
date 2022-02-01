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

inline const NLogging::TLogger ControllerLogger("Controller");
inline const NLogging::TLogger ControllerAgentLogger("ControllerAgent");
inline const NLogging::TLogger ControllerEventLogger("ControllerEventLog");
inline const NLogging::TLogger ControllerFeatureStructuredLogger("ControllerFeatureStructuredLog");

inline const NProfiling::TProfiler ControllerAgentProfiler("/controller_agent");

////////////////////////////////////////////////////////////////////////////////

using TOperationIdToControllerMap = THashMap<TOperationId, IOperationControllerPtr>;
using TOperationIdToWeakControllerMap = THashMap<TOperationId, IOperationControllerWeakPtr>;

////////////////////////////////////////////////////////////////////////////////

using TStartedJobCounter = THashMap<std::tuple<EJobType, TString>, NProfiling::TCounter>;
using TAbortedJobCounter = THashMap<std::tuple<EJobType, EAbortReason, TString>, NProfiling::TCounter>;
using TAbortedJobByErrorCounter = THashMap<std::tuple<EJobType, int, TString>, NProfiling::TCounter>;
using TFailedJobCounter = THashMap<std::tuple<EJobType, TString>, NProfiling::TCounter>;
using TCompletedJobCounter = THashMap<std::tuple<EJobType, EInterruptReason, TString>, NProfiling::TCounter>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

