#include "config.h"

namespace NYT::NHiveClient {

////////////////////////////////////////////////////////////////////////////////

void TClusterDirectorySynchronizerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("sync_period", &TThis::SyncPeriod)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("expire_after_successful_update_time", &TThis::ExpireAfterSuccessfulUpdateTime)
        .Alias("success_expiration_time")
        .Default(TDuration::Seconds(15));
    registrar.Parameter("expire_after_failed_update_time", &TThis::ExpireAfterFailedUpdateTime)
        .Alias("failure_expiration_time")
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

void TCellDirectorySynchronizerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("sync_period", &TThis::SyncPeriod)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("sync_period_splay", &TThis::SyncPeriodSplay)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("sync_rpc_timeout", &TThis::SyncRpcTimeout)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("sync_cells_with_secondary_masters", &TThis::SyncCellsWithSecondaryMasters)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
