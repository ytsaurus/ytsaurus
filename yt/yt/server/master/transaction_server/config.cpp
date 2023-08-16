#include "config.h"

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

void TTransactionPresenceCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("finished_transaction_eviction_delay", &TThis::FinishedTransactionEvictionDelay)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("eviction_check_period", &TThis::EvictionCheckPeriod)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("max_evicted_transactions_per_check", &TThis::MaxEvictedTransactionsPerCheck)
        .Default(25000)
        .GreaterThanOrEqual(0);
}

////////////////////////////////////////////////////////////////////////////////

void TBoomerangTrackerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("stuck_boomerang_wave_expiration_time", &TThis::StuckBoomerangWaveExpirationTime)
        .Default(TDuration::Minutes(3));
    registrar.Parameter("stuck_boomerang_wave_expiration_check_period", &TThis::StuckBoomerangWaveExpirationCheckPeriod)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("max_expired_boomerang_wave_removals_per_check", &TThis::MaxExpiredBoomerangWaveRemovalsPerCheck)
        .Default(1000)
        .GreaterThanOrEqual(0);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicTransactionManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_transaction_timeout", &TThis::MaxTransactionTimeout)
        .Default(TDuration::Minutes(60));
    registrar.Parameter("max_transaction_depth", &TThis::MaxTransactionDepth)
        .GreaterThan(0)
        .Default(32);
    registrar.Parameter("enable_lazy_transaction_replication", &TThis::EnableLazyTransactionReplication)
        .Default(true);
    registrar.Parameter("transaction_presence_cache", &TThis::TransactionPresenceCache)
        .DefaultNew();
    registrar.Parameter("boomerang_tracker", &TThis::BoomerangTracker)
        .DefaultNew();
    registrar.Parameter("profiling_period", &TThis::ProfilingPeriod)
        .Default(DefaultProfilingPeriod);

    // COMPAT(gritukan): This is an emergency button to restore old master transactions
    // behavior.
    registrar.Parameter("ignore_cypress_transactions", &TThis::IgnoreCypressTransactions)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
