#include "config.h"

namespace NYT::NChaosClient {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TChaosResidencyCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("use_has_chaos_object", &TThis::UseHasChaosObject)
        .Default(false);
}

void TChaosResidencyCacheConfig::ApplyDynamicInplace(const TChaosResidencyCacheDynamicConfigPtr& dynamicConfig)
{
    TAsyncExpiringCacheConfig::ApplyDynamicInplace(dynamicConfig);
}

TChaosResidencyCacheConfigPtr TChaosResidencyCacheConfig::ApplyDynamic(
    const TChaosResidencyCacheDynamicConfigPtr& dynamicConfig) const
{
    auto config = CloneYsonStruct(MakeStrong(this));
    config->ApplyDynamicInplace(dynamicConfig);
    config->Postprocess();
    return config;
}

////////////////////////////////////////////////////////////////////////////////

void TChaosResidencyCacheDynamicConfig::Register(TRegistrar /*registrar*/)
{ }

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

void TChaosObjectChannelConfig::Register(TRegistrar registrar)
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

void TChaosReplicationCardUpdatesBatcherConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(true);
    registrar.Parameter("flush_period", &TThis::FlushPeriod)
        .Default(TDuration::Seconds(1));
}


void TChaosReplicationCardUpdatesBatcherConfig::ApplyDynamicInplace(
    const TChaosReplicationCardUpdatesBatcherDynamicConfigPtr& dynamicConfig)
{
    UpdateYsonStructField(Enable, dynamicConfig->Enable);
    UpdateYsonStructField(FlushPeriod, dynamicConfig->FlushPeriod);
}

TChaosReplicationCardUpdatesBatcherConfigPtr TChaosReplicationCardUpdatesBatcherConfig::ApplyDynamic(
    const TChaosReplicationCardUpdatesBatcherDynamicConfigPtr& dynamicConfig) const
{
    auto config = CloneYsonStruct(MakeStrong(this));
    config->ApplyDynamicInplace(dynamicConfig);
    config->Postprocess();
    return config;
}

////////////////////////////////////////////////////////////////////////////////

void TChaosReplicationCardUpdatesBatcherDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Optional();
    registrar.Parameter("flush_period", &TThis::FlushPeriod)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
