#pragma once

#include "private.h"

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion();
bool ValidateSnapshotVersion(int version);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESnapshotVersion,
    ((JobSplitterSpeculative)         (300104))
    ((InputOutputTableLock)           (300105))
    ((PrepareRootFSDuration)          (300106))
    // It is intentionally differ from version in stable/19.5.
    ((JobMetricsAggregationType)      (300107))
    ((SaveJobPhase)                   (300108))
    ((JobSplitterPrepareDuration)     (300109))
    ((ForceAdvanceBefore19_6)         (300150))
    ((JobMetricsByOperationState)     (300151))
    ((OutputToDynamicTables)          (300152))
    ((VanillaRestartCompletedJobs)    (300153))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
