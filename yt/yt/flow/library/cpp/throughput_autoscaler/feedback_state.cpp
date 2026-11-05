#include "feedback_state.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TThroughputFeedbackBaseline::Register(TRegistrar registrar)
{
    registrar.Parameter("partition_count", &TThis::PartitionCount);
    registrar.Parameter("capacity", &TThis::Capacity);
    registrar.Parameter("busy_partition_share", &TThis::BusyPartitionShare)
        .Default();
}

void TThroughputExplorationState::Register(TRegistrar registrar)
{
    registrar.Parameter("origin_partition_count", &TThis::OriginPartitionCount);
    registrar.Parameter("origin_processed", &TThis::OriginProcessed);
    registrar.Parameter("best_processed", &TThis::BestProcessed);
}

void TThroughputFeedbackState::Register(TRegistrar registrar)
{
    registrar.Parameter("anchor", &TThis::Anchor)
        .Default();
    registrar.Parameter("exploration", &TThis::Exploration)
        .Default();
    registrar.Parameter("phase", &TThis::Phase)
        .Default(EThroughputSearchPhase::Idle);
    registrar.Parameter("pending_partition_count", &TThis::PendingPartitionCount)
        .Default();
    registrar.Parameter("observed_partition_count", &TThis::ObservedPartitionCount)
        .Default();
    registrar.Parameter("retry_warmup_period", &TThis::RetryWarmupPeriod)
        .Default();
    registrar.Parameter("capacity_degraded", &TThis::CapacityDegraded)
        .Default();
    registrar.Parameter("reset_requested", &TThis::ResetRequested)
        .Default();
    registrar.Parameter("pipeline_reset_generation", &TThis::PipelineResetGeneration)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
