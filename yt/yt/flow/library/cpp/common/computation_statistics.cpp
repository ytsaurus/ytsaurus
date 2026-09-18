#include "computation_statistics.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TBatchStatistics& TBatchStatistics::operator+=(const TBatchStatistics& other)
{
    Count += other.Count;
    ByteSize += other.ByteSize;
    return *this;
}

void TProcessingObservation::Register(TRegistrar registrar)
{
    registrar.Parameter("sequence", &TThis::Sequence);
    registrar.Parameter("spec_generation", &TThis::SpecGeneration);
    registrar.Parameter("captured_at", &TThis::CapturedAt);
    registrar.Parameter("observation_duration", &TThis::ObservationDuration);
    registrar.Parameter("processed_count", &TThis::ProcessedCount);
    registrar.Parameter("processed_byte_size", &TThis::ProcessedByteSize);
    registrar.Parameter("processing_time", &TThis::ProcessingTime);
    registrar.Parameter("input_waiting_time", &TThis::InputWaitingTime);
    registrar.Parameter("output_waiting_time", &TThis::OutputWaitingTime);
    registrar.Parameter("other_waiting_time", &TThis::OtherWaitingTime);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
