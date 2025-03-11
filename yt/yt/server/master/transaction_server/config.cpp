#include "config.h"

namespace NYT::NTransactionServer {

using namespace NObjectClient;

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
    registrar.Parameter("transaction_presence_cache", &TThis::TransactionPresenceCache)
        .DefaultNew();
    registrar.Parameter("boomerang_tracker", &TThis::BoomerangTracker)
        .DefaultNew();
    registrar.Parameter("profiling_period", &TThis::ProfilingPeriod)
        .Default(DefaultProfilingPeriod);
    registrar.Parameter("check_transaction_is_compatible_with_method", &TThis::CheckTransactionIsCompatibleWithMethod)
        .Default(true);
    registrar.Parameter("alert_transaction_is_not_compatible_with_method", &TThis::AlertTransactionIsNotCompatibleWithMethod)
        .Default(false);

    THashMap<EObjectType, THashSet<TString>> defaultWhitelist;
    defaultWhitelist[EObjectType::UploadTransaction] = {
        "BeginUpload",
        "EndUpload",
        "Get",
        "GetUploadParams",
    };
    defaultWhitelist[EObjectType::UploadNestedTransaction] = defaultWhitelist[EObjectType::UploadTransaction];
    defaultWhitelist[EObjectType::SystemTransaction] = {
        "Create",
        "Get",
    };
    defaultWhitelist[EObjectType::SystemNestedTransaction] = defaultWhitelist[EObjectType::SystemTransaction];

    registrar.Parameter("transaction_type_to_method_whitelist", &TThis::TransactionTypeToMethodWhitelist)
        .Default(defaultWhitelist);

    registrar.Parameter("throw_on_lease_revocation", &TThis::ThrowOnLeaseRevocation)
        .Default(false);

    // COMPAT(gritukan): This is an emergency button to restore old master transactions
    // behavior.
    registrar.Parameter("ignore_cypress_transactions", &TThis::IgnoreCypressTransactions)
        .Default(false);

    registrar.Parameter("forbid_transaction_actions_for_cypress_transactions", &TThis::ForbidTransactionActionsForCypressTransactions)
        .Default(false)
        .DontSerializeDefault();

    registrar.Parameter("enable_start_foreign_transaction_fixes", &TThis::EnableStartForeignTransactionFixes)
        .Default(false)
        .DontSerializeDefault();

    registrar.Parameter("enable_cypress_mirrorred_to_sequoia_prerequisite_transaction_validation_via_leases", &TThis::EnableCypressMirroredToSequoiaPrerequisiteTransactionValidationViaLeases)
        // COMPAT(cherepashka)
        .Alias("enable_prerequisite_transaction_validation_via_leases")
        .Default(false);

    registrar.Parameter("enable_non_strict_externalized_transaction_usage", &TThis::EnableNonStrictExternalizedTransactionUsage)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
