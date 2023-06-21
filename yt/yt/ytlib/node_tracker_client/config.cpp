#include "config.h"

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

void TNodeDirectorySynchronizerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("sync_period", &TThis::SyncPeriod)
        .Default(TDuration::Minutes(2));

    registrar.Parameter("expire_after_successful_update_time", &TThis::ExpireAfterSuccessfulUpdateTime)
        .Alias("success_expiration_time")
        .Default(TDuration::Minutes(2));
    registrar.Parameter("expire_after_failed_update_time", &TThis::ExpireAfterFailedUpdateTime)
        .Alias("failure_expiration_time")
        .Default(TDuration::Minutes(2));

    registrar.Parameter("cache_sticky_group_size", &TThis::CacheStickyGroupSize)
        .Default();

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
