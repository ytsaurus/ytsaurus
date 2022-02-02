#include "config.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TWorkloadConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("workload_descriptor", &TThis::WorkloadDescriptor)
        .Default(TWorkloadDescriptor(EWorkloadCategory::UserBatch));
}

////////////////////////////////////////////////////////////////////////////////

TDiscoveryConfig::TDiscoveryConfig(NYPath::TYPath directoryPath)
{
    RegisterParameter("directory", Directory)
        .Default(directoryPath);

    RegisterParameter("update_period", UpdatePeriod)
        .Default(TDuration::Seconds(30));

    RegisterParameter("ban_timeout", BanTimeout)
        .Default(TDuration::Seconds(60));
    RegisterParameter("transaction_timeout", TransactionTimeout)
        .Default(TDuration::Seconds(15));
    RegisterParameter("transaction_ping_period", TransactionPingPeriod)
        .Default(TDuration::Seconds(5));
    RegisterParameter("skip_unlocked_participants", SkipUnlockedParticipants)
        .Default(true);

    RegisterParameter("lock_node_timeout", LockNodeTimeout)
        .Default(TDuration::Minutes(5));

    RegisterParameter("read_from", ReadFrom)
        .Default(NApi::EMasterChannelKind::Follower);
    RegisterParameter("master_cache_expire_time", MasterCacheExpireTime)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
