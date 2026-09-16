#include "computation_statistics.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TBatchStatistics& TBatchStatistics::operator+=(const TBatchStatistics& other)
{
    Count += other.Count;
    ByteSize += other.ByteSize;
    return *this;
}

void TPartitionProcessingRates::Register(TRegistrar registrar)
{
    registrar.Parameter("processed", &TThis::Processed);
    registrar.Parameter("capacity", &TThis::Capacity)
        .Default();
}

void TComputationProcessingRates::Register(TRegistrar registrar)
{
    registrar.Parameter("rate_1m", &TThis::Rate1m)
        .Default();
    registrar.Parameter("rate_10m", &TThis::Rate10m)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
