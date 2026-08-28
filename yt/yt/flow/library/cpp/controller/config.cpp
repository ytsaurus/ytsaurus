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
        .Default(TDuration::Minutes(10));
    registrar.Parameter("lease_ping_period", &TThis::LeasePingPeriod)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("max_concurrent_requests", &TThis::MaxConcurrentRequests)
        .Default(500);
}

////////////////////////////////////////////////////////////////////////////////

void TElectionBackendConfigBase::Register(TRegistrar registrar)
{
    registrar.Parameter("lock_acquisition_period", &TThis::LockAcquisitionPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("leader_cache_update_period", &TThis::LeaderCacheUpdatePeriod)
        .Default(TDuration::Seconds(1));
}

////////////////////////////////////////////////////////////////////////////////

void TCypressElectionBackendConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("transaction_timeout", &TThis::TransactionTimeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("transaction_ping_period", &TThis::TransactionPingPeriod)
        .Default(TDuration::Seconds(1));
}

////////////////////////////////////////////////////////////////////////////////

void TDyntableElectionBackendConfig::Register(TRegistrar registrar)
{
    // The lease must comfortably survive chaos commit latency and transient conflict storms:
    // a renew attempt is retried every lock_acquisition_period, so the ttl/detach window should
    // cover many attempts even when each one takes seconds.
    registrar.Parameter("leader_lease_ttl", &TThis::LeaderLeaseTtl)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("detach_timeout", &TThis::DetachTimeout)
        .Default(TDuration::Minutes(1));
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

    // NB: A default-constructed polymorphic struct holds nothing, so an absent "election_manager"
    // would leave the connector without a backend config; construct the default one explicitly.
    registrar.Parameter("election_manager", &TThis::ElectionManager)
        .DefaultCtor([] {
            return TElectionManagerConfig(EElectionBackend::Cypress);
        });

    registrar.Parameter("persisted_state_manager", &TThis::PersistedStateManager)
        .Alias("state_manager")
        .DefaultNew();

    registrar.Parameter("lease_manager", &TThis::LeaseManager)
        .DefaultNew();

    registrar.Parameter("controller_service", &TThis::ControllerService)
        .DefaultNew();

    registrar.Parameter("bus", &TThis::Bus)
        .DefaultNew();

    registrar.Postprocessor([] (TThis* config) {
        if (config->ElectionManager.GetType() != EElectionBackend::Dyntable) {
            return;
        }
        // Nothing refreshes the leader row between two fenced commits, so the ttl has to cover
        // the scheduling cadence with room for a slow iteration and its commit latency. The
        // widest regular gap is the no-spec backoff, but a long iteration produces the same one.
        const auto& backendConfig = config->ElectionManager.GetConcrete<TDyntableElectionBackendConfig>();
        auto cadence = std::max(config->SchedulerPeriod, NoSpecIterationBackoff);
        auto minLeaderLeaseTtl = MinLeaderLeaseTtlToCadenceRatio * cadence;
        if (backendConfig->LeaderLeaseTtl < minLeaderLeaseTtl) {
            THROW_ERROR_EXCEPTION("%Qv is too small for the scheduling cadence", "leader_lease_ttl")
                .With("leader_lease_ttl", backendConfig->LeaderLeaseTtl)
                .With("scheduler_period", config->SchedulerPeriod)
                .With("min_leader_lease_ttl", minLeaderLeaseTtl);
        }

        // The other end of the same rope. A handover has to finish before the deadline the old
        // leader wrote runs out, or every job of the pipeline loses its fence over a routine
        // change of leader: the replica first waits out the leader lease, then reads the lease
        // table, and only then touches the deadline — while the deadline it inherited may already
        // be two thirds spent, since the old leader refreshes it at a fraction of the timeout.
        auto minLeaseTimeout = MinLeaseTimeoutToLeaderLeaseTtlRatio * backendConfig->LeaderLeaseTtl;
        if (config->LeaseManager->LeaseTimeout < minLeaseTimeout) {
            THROW_ERROR_EXCEPTION("%Qv is too small for %Qv", "lease_timeout", "leader_lease_ttl")
                .With("lease_timeout", config->LeaseManager->LeaseTimeout)
                .With("leader_lease_ttl", backendConfig->LeaderLeaseTtl)
                .With("min_lease_timeout", minLeaseTimeout);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
