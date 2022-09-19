#include "config.h"

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

void TChaosCellDirectorySynchronizerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("sync_period", &TThis::SyncPeriod)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("sync_period_splay", &TThis::SyncPeriodSplay)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("sync_all_chaos_cells", &TThis::SyncAllChaosCells)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TReplicationCardChannelConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("rpc_acknowledgement_timeout", &TThis::RpcAcknowledgementTimeout)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
