#include "config.h"

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

void TSecurityManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("user_throttler", &TThis::UserThrottler)
        .DefaultNew();

    registrar.Parameter("alert_on_ref_counter_mismatch", &TThis::AlertOnAccountRefCounterMismatch)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicSecurityManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("account_statistics_gossip_period", &TThis::AccountStatisticsGossipPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("request_rate_smoothing_period", &TThis::RequestRateSmoothingPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("account_master_memory_usage_update_period", &TThis::AccountMasterMemoryUsageUpdatePeriod)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("enable_delayed_membership_closure_recomputation", &TThis::EnableDelayedMembershipClosureRecomputation)
        .Default(true);
    registrar.Parameter("membership_closure_recomputation_period", &TThis::MembershipClosureRecomputePeriod)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("enable_access_log", &TThis::EnableAccessLog)
        .Default(true);
    registrar.Parameter("enable_master_memory_usage_validation", &TThis::EnableMasterMemoryUsageValidation)
        .Default(false);
    registrar.Parameter("enable_master_memory_usage_account_overcommit_validation", &TThis::EnableMasterMemoryUsageAccountOvercommitValidation)
        .Default(false);
    registrar.Parameter("enable_tablet_resource_validation", &TThis::EnableTabletResourceValidation)
        .Default(true);

    registrar.Parameter("enable_distributed_throttler", &TThis::EnableDistributedThrottler)
        .Default(false);

    registrar.Parameter("max_account_subtree_size", &TThis::MaxAccountSubtreeSize)
        .Default(1000);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
