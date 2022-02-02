#pragma once

#include "workload.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/ytree/yson_serializable.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TWorkloadConfig
    : public virtual NYTree::TYsonStruct
{
public:
    TWorkloadDescriptor WorkloadDescriptor;

    REGISTER_YSON_STRUCT(TWorkloadConfig);

    static void Register(TRegistrar registrar);
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

    explicit TDiscoveryConfig(NYPath::TYPath directoryPath);
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
