#include "config.h"

namespace NYT::NClickHouseServer {

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

} // namespace NYT::NClickHouseServer
