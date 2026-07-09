#include "config.h"

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/net/local_address.h>
#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/library/cypress_election/config.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

void TPersistedStateManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("timeout", &TThis::Timeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("max_reads_per_transaction", &TThis::MaxReadsPerTransaction)
        .Default(10000);
    registrar.Parameter("max_writes_per_transaction", &TThis::MaxWritesPerTransaction)
        .Default(10000);
}

////////////////////////////////////////////////////////////////////////////////

void TLeaseManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("lease_timeout", &TThis::LeaseTimeout)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("lease_ping_period", &TThis::LeasePingPeriod)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("lease_check_period", &TThis::LeaseCheckPeriod)
        .Default(TDuration::Minutes(2));
    registrar.Parameter("lease_check_period_jitter", &TThis::LeaseCheckPeriodJitter)
        .Default(0.5);
    registrar.Parameter("max_concurrent_requests", &TThis::MaxConcurrentRequests)
        .Default(500);
}

////////////////////////////////////////////////////////////////////////////////

void TElectionManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("transaction_timeout", &TThis::TransactionTimeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("transaction_ping_period", &TThis::TransactionPingPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("lock_acquisition_period", &TThis::LockAcquisitionPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("leader_cache_update_period", &TThis::LeaderCacheUpdatePeriod)
        .Default(TDuration::Seconds(1));
}

////////////////////////////////////////////////////////////////////////////////

void TControllerServiceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("set_spec_retry_count", &TThis::SetSpecRetryCount)
        .Default(3);
    registrar.Parameter("set_spec_retry_period", &TThis::SetSpecRetryPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("tables_throttler", &TThis::TablesThrottler)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TControllerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("controller_threads", &TThis::ControllerThreads)
        .Default(5);

    registrar.Parameter("orchid_update_period", &TThis::OrchidUpdatePeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("warm_up_time", &TThis::WarmUpTime)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("scheduler_period", &TThis::SchedulerPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("cache_period", &TThis::CachePeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("feedback_period", &TThis::FeedbackPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("metrics_period", &TThis::MetricsPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("write_own_retryable_errors_period", &TThis::WriteOwnRetryableErrorsPeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("publish_retry_period", &TThis::PublishRetryPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("publish_timeout", &TThis::PublishTimeout)
        .Default(TDuration::Minutes(120));

    registrar.Parameter("election_manager", &TThis::ElectionManager)
        .DefaultNew();

    registrar.Parameter("persisted_state_manager", &TThis::PersistedStateManager)
        .Alias("state_manager")
        .DefaultNew();

    registrar.Parameter("lease_manager", &TThis::LeaseManager)
        .DefaultNew();

    registrar.Parameter("controller_service", &TThis::ControllerService)
        .DefaultNew();

    registrar.Parameter("bus", &TThis::Bus)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
