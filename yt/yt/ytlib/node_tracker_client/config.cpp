#include "config.h"

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

TNodeDirectorySynchronizerConfig::TNodeDirectorySynchronizerConfig()
{
    RegisterParameter("sync_period", SyncPeriod)
        .Default(TDuration::Minutes(2));

    RegisterParameter("expire_after_successful_update_time", ExpireAfterSuccessfulUpdateTime)
        .Alias("success_expiration_time")
        .Default(TDuration::Minutes(2));
    RegisterParameter("expire_after_failed_update_time", ExpireAfterFailedUpdateTime)
        .Alias("failure_expiration_time")
        .Default(TDuration::Minutes(2));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
