#include "spec.h"

#include "joiner.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TServiceLogParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("table_joiner", &TThis::TableJoiner)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicServiceLogParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("desired_partition_count", &TThis::DesiredPartitionCount)
        .Default(5);
    registrar.Parameter("desired_cycle_time", &TThis::DesiredCycleTime)
        .Default(TDuration::Hours(12));
    registrar.Parameter("throttler_period", &TThis::ThrottlerPeriod)
        .Default(TDuration::Seconds(10));
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicServiceLogPartitionSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("range", &TThis::Range);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
