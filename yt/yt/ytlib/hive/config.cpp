#include "config.h"

namespace NYT::NHiveClient {

////////////////////////////////////////////////////////////////////////////////

TClusterDirectorySynchronizerConfig::TClusterDirectorySynchronizerConfig()
{
    RegisterParameter("sync_period", SyncPeriod)
        .Default(TDuration::Seconds(60));

    RegisterParameter("expire_after_successful_update_time", ExpireAfterSuccessfulUpdateTime)
        .Alias("success_expiration_time")
        .Default(TDuration::Seconds(15));
    RegisterParameter("expire_after_failed_update_time", ExpireAfterFailedUpdateTime)
        .Alias("failure_expiration_time")
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

TCellDirectorySynchronizerConfig::TCellDirectorySynchronizerConfig()
{
    RegisterParameter("sync_period", SyncPeriod)
        .Default(TDuration::Seconds(15));
    RegisterParameter("sync_period_splay", SyncPeriodSplay)
        .Default(TDuration::Seconds(5));
    RegisterParameter("sync_rpc_timeout", SyncRpcTimeout)
        .Default(TDuration::Seconds(60));

    RegisterParameter("sync_cells_with_secondary_masters", SyncCellsWithSecondaryMasters)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
