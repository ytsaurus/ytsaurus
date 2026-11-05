#include "config.h"

namespace NYT::NDiscoveryClient {

////////////////////////////////////////////////////////////////////////////////

void TDiscoveryConnectionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("server_ban_timeout", &TThis::ServerBanTimeout)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

void TMemberClientConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("heartbeat_period", &TThis::HeartbeatPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("attribute_update_period", &TThis::AttributeUpdatePeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("lease_timeout", &TThis::LeaseTimeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("max_failed_heartbeats_on_startup", &TThis::MaxFailedHeartbeatsOnStartup)
        .Default(10);
    registrar.Parameter("write_quorum", &TThis::WriteQuorum)
        .GreaterThan(0)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TDiscoveryClientConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("read_quorum", &TThis::ReadQuorum)
        .GreaterThan(0)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TDiscoveryBaseConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("group_id", &TThis::GroupId)
        .Default();

    registrar.Parameter("update_period", &TThis::UpdatePeriod)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("ban_timeout", &TThis::BanTimeout)
        .Default(TDuration::Seconds(60));
}

////////////////////////////////////////////////////////////////////////////////

void TDiscoveryConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("version", &TThis::Version)
        .InRange(1, 2)
        .Default(2);

    registrar.Parameter("discovery_readiness_timeout", &TThis::DiscoveryReadinessTimeout)
        .Default(TDuration::Seconds(1));
    registrar.Preprocessor([] (TThis* config) {
        config->ReadQuorum = 1;
        config->WriteQuorum = 1;
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient

