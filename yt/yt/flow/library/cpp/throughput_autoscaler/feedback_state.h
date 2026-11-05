#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <util/datetime/base.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TThroughputFeedbackBaseline
    : public NYTree::TYsonStructLite
{
    i64 PartitionCount = 0;
    double Capacity = 0;
    double BusyPartitionShare = 0;

    REGISTER_YSON_STRUCT_LITE(TThroughputFeedbackBaseline);

    static void Register(TRegistrar registrar);
};

struct TThroughputExplorationState
    : public NYTree::TYsonStructLite
{
    i64 OriginPartitionCount = 0;
    double OriginProcessed = 0;
    double BestProcessed = 0;

    REGISTER_YSON_STRUCT_LITE(TThroughputExplorationState);

    static void Register(TRegistrar registrar);
};

struct TThroughputFeedbackState
    : public NYTree::TYsonStructLite
{
    std::optional<TThroughputFeedbackBaseline> Anchor;
    std::optional<TThroughputExplorationState> Exploration;
    EThroughputSearchPhase Phase = EThroughputSearchPhase::Idle;
    std::optional<i64> PendingPartitionCount;
    i64 ObservedPartitionCount = 0;
    TDuration RetryWarmupPeriod;
    bool CapacityDegraded = false;
    bool ResetRequested = false;
    i64 PipelineResetGeneration = 0;

    REGISTER_YSON_STRUCT_LITE(TThroughputFeedbackState);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
