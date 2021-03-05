#pragma once

#include "workload.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TWorkloadConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TWorkloadDescriptor WorkloadDescriptor;

    TWorkloadConfig()
    {
        RegisterParameter("workload_descriptor", WorkloadDescriptor)
            .Default(TWorkloadDescriptor(EWorkloadCategory::UserBatch));
    }
};

DEFINE_REFCOUNTED_TYPE(TWorkloadConfig)

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryConfig
    : public NYTree::TYsonSerializable
{
public:
    NYPath::TYPath Directory;
    TDuration UpdatePeriod;
    TDuration BanTimeout;
    TDuration TransactionTimeout;
    TDuration TransactionPingPeriod;
    bool SkipUnlockedParticipants;

    //! How long a clique node can live without a transaction lock after creation.
    //! Mostly for test configurations.
    TDuration LockNodeTimeout;

    NApi::EMasterChannelKind ReadFrom;
    //! Used only for ReadFrom == Cache.
    TDuration MasterCacheExpireTime;

    TDiscoveryConfig() = default;

    explicit TDiscoveryConfig(NYPath::TYPath directoryPath)
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
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
