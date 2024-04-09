#include "config.h"

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

void TLogicalTimeRegistryConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("eviction_period", &TThis::EvictionPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("expiration_timeout", &TThis::ExpirationTimeout)
        .Default(TDuration::Minutes(1));
}

////////////////////////////////////////////////////////////////////////////////

void THiveManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("ping_period", &TThis::PingPeriod)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("idle_post_period", &TThis::IdlePostPeriod)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("post_batching_period", &TThis::PostBatchingPeriod)
        .Default(TDuration::MilliSeconds(10));
    registrar.Parameter("ping_rpc_timeout", &TThis::PingRpcTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("send_rpc_timeout", &TThis::SendRpcTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("post_rpc_timeout", &TThis::PostRpcTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("max_messages_per_post", &TThis::MaxMessagesPerPost)
        .Default(16384);
    registrar.Parameter("max_bytes_per_post", &TThis::MaxBytesPerPost)
        .Default(16_MB);
    registrar.Parameter("cached_channel_timeout", &TThis::CachedChannelTimeout)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("sync_delay", &TThis::SyncDelay)
        .Default(TDuration::MilliSeconds(10));
    registrar.Parameter("sync_timeout", &TThis::SyncTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("logical_time_registry", &TThis::LogicalTimeRegistry)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////


void TCellDirectorySynchronizerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("sync_period", &TThis::SyncPeriod)
        .Default(TDuration::Seconds(3));
}

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

} // namespace NYT::NHiveServer
