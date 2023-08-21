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

void TDiscoveryV1Config::Register(TRegistrar registrar)
{
    registrar.Parameter("directory", &TThis::Directory)
        .Default();

    registrar.Parameter("transaction_timeout", &TThis::TransactionTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("transaction_ping_period", &TThis::TransactionPingPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("skip_unlocked_participants", &TThis::SkipUnlockedParticipants)
        .Default(true);

    registrar.Parameter("lock_node_timeout", &TThis::LockNodeTimeout)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("read_from", &TThis::ReadFrom)
        .Default(NApi::EMasterChannelKind::Follower);
    registrar.Parameter("master_cache_expire_time", &TThis::MasterCacheExpireTime)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

void TDiscoveryV2Config::Register(TRegistrar registrar)
{
    registrar.Preprocessor([] (TThis* config) {
        config->ReadQuorum = 1;
        config->WriteQuorum = 1;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDiscoveryConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("version", &TThis::Version)
        .InRange(1, 2)
        .Default(2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
