#pragma once

#include <yt/yt/flow/library/cpp/common/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TProcessingObservationWindow;

DEFINE_ENUM(EThroughputSearchPhase,
    (Idle)
    (FirstProbe)
    (SecondProbe)
    (Plateau)
    (Recalibrating)
    (Releasing)
    (RatePlateau)
    (ExplorationLimited)
);

DEFINE_ENUM(EThroughputPartitioningAction,
    (Regular)
    (ReturnToWorkingPoint)
);

struct TThroughputAutoscalerDecision
{
    i64 TargetPartitionCount = 0;
    EThroughputPartitioningAction Action = EThroughputPartitioningAction::Regular;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
