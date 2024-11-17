#include "config.h"

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

void TChaosResidencyCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("is_client_mode_active", &TThis::IsClientModeActive)
        .Default(false);
}

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

void TReplicationCardsWatcherConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("poll_expiration_time", &TThis::PollExpirationTime)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("gone_cards_expiration_time", &TThis::GoneCardsExpirationTime)
        .Default(TDuration::Minutes(10));
    registrar.Parameter("replication_card_keep_alive_period", &TThis::ExpirationSweepPeriod)
        .Default(TDuration::Seconds(10));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
