#include "config.h"

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

void TMutationIdempotizerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(true);
    registrar.Parameter("expiration_time", &TThis::ExpirationTime)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("expiration_check_period", &TThis::ExpirationCheckPeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("max_expired_mutation_id_removals_per_commit", &TThis::MaxExpiredMutationIdRemovalsPerCommit)
        .Default(50000);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicObjectManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_weight_per_gc_sweep", &TThis::MaxWeightPerGCSweep)
        .Default(100000);
    registrar.Parameter("gc_sweep_period", &TThis::GCSweepPeriod)
        .Default(TDuration::MilliSeconds(1000));
    registrar.Parameter("object_removal_cells_sync_period", &TThis::ObjectRemovalCellsSyncPeriod)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("mutation_idempotizer", &TThis::MutationIdempotizer)
        .DefaultNew();
    registrar.Parameter("reserved_attributes", &TThis::ReservedAttributes)
        .Default();
    registrar.Parameter("yson_string_intern_length_threshold", &TThis::YsonStringInternLengthThreshold)
        .Default(DefaultYsonStringInternLengthThreshold)
        .InRange(DefaultYsonStringInternLengthThreshold, 1_GB);
    registrar.Parameter("profiling_period", &TThis::ProfilingPeriod)
        .Default(DefaultProfilingPeriod);
}

////////////////////////////////////////////////////////////////////////////////

void TObjectServiceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("yield_timeout", &TThis::YieldTimeout)
        .Default(TDuration::MilliSeconds(10));

    registrar.Parameter("sticky_user_error_expire_time", &TThis::StickyUserErrorExpireTime)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("cross_cell_sync_delay", &TThis::CrossCellSyncDelay)
        .Default(TDuration::MilliSeconds(10));

    registrar.Parameter("timeout_backoff_lead_time", &TThis::TimeoutBackoffLeadTime)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("default_execute_timeout", &TThis::DefaultExecuteTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("forwarded_request_timeout_reserve", &TThis::ForwardedRequestTimeoutReserve)
        .Default(TDuration::Seconds(3));

    registrar.Parameter("master_cache", &TThis::MasterCache)
        .DefaultNew();

    registrar.Parameter("enable_local_read_executor", &TThis::EnableLocalReadExecutor)
        .Default(false);
    registrar.Parameter("enable_local_read_busy_wait", &TThis::EnableLocalReadBusyWait)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicObjectServiceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_two_level_cache", &TThis::EnableTwoLevelCache)
        .Default(true);
    registrar.Parameter("enable_local_read_executor", &TThis::EnableLocalReadExecutor)
        .Default(false);
    registrar.Parameter("local_read_worker_count", &TThis::LocalReadWorkerCount)
        .GreaterThan(0)
        .Default(4);
    registrar.Parameter("schedule_reply_retry_backoff", &TThis::ScheduleReplyRetryBackoff)
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("local_read_executor_quantum_duration", &TThis::LocalReadExecutorQuantumDuration)
        .Default(TDuration::MilliSeconds(10));

    registrar.Parameter("process_sessions_period", &TThis::ProcessSessionsPeriod)
        .Default(TDuration::MilliSeconds(10));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
