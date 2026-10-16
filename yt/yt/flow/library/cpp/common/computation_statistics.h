#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TBatchStatistics
{
    i64 Count = 0;
    i64 ByteSize = 0;

    TBatchStatistics& operator+=(const TBatchStatistics& other);
};

//! Cumulative processing metrics since the start of the job.
struct TProcessingObservation
    : public NYTree::TYsonStruct
{
    i64 Sequence = 0;
    i64 SpecGeneration = 0;
    TInstant CapturedAt;
    TDuration ObservationDuration;
    // Processing work includes skipped inputs, timers and visits, not unique terminal completions.
    i64 ProcessedCount = 0;
    i64 ProcessedByteSize = 0;
    TDuration ProcessingTime;
    TDuration InputWaitingTime;
    TDuration OutputWaitingTime;
    TDuration OtherWaitingTime;

    REGISTER_YSON_STRUCT(TProcessingObservation);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProcessingObservation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
