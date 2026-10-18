#pragma once

#include "processing_measurement.h"

#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/traverse.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TThroughputStreamObservation
{
    std::optional<double> Ready;
    std::optional<double> Offered;
};

struct TThroughputComputationObservation
{
    i64 PartitionCount = 0;
    TInstant ReadyAt;
    bool Repartitioning = false;
    bool AllJobsHealthy = false;
    TDuration ActualBatchDuration;
    std::optional<TThroughputProcessingMeasurement> Measurement;
    std::optional<TThroughputProcessingMeasurement> RecoveryMeasurement;
    THashMap<TStreamId, TThroughputStreamObservation> Streams;
    TLineageRatios LineageRatios;
};

struct TThroughputMeasurementRequest
{
    TComputationSpecPtr Spec;
    TDuration Period;
    std::optional<TDuration> RecoveryPeriod;
};

using TThroughputMeasurementRequests = THashMap<TComputationId, TThroughputMeasurementRequest>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
