#include "config.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TWorkloadConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("workload_descriptor", &TThis::WorkloadDescriptor)
        .Default(TWorkloadDescriptor(EWorkloadCategory::UserBatch));
}

////////////////////////////////////////////////////////////////////////////////

void TDiscoveryConfig::DoRegister(TRegistrar registrar, NYPath::TYPath directoryPath)
{
    registrar.Parameter("directory", &TThis::Directory)
        .Default(directoryPath);

    registrar.Parameter("update_period", &TThis::UpdatePeriod)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("ban_timeout", &TThis::BanTimeout)
        .Default(TDuration::Seconds(60));
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

} // namespace NYT
