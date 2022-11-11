#pragma once

#include "public.h"

#include <yt/yt/ytlib/discovery_client/config.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryBaseConfig
    : public virtual NYTree::TYsonStruct
{
public:
    TString GroupId;
    TDuration UpdatePeriod;
    TDuration BanTimeout;

    REGISTER_YSON_STRUCT(TDiscoveryBaseConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryBaseConfig)

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryV1Config
    : public virtual TDiscoveryBaseConfig
{
public:
    NYPath::TYPath Directory;
    TDuration TransactionTimeout;
    TDuration TransactionPingPeriod;
    bool SkipUnlockedParticipants;

    //! How long a clique node can live without a transaction lock after creation.
    //! Mostly for test configurations.
    TDuration LockNodeTimeout;

    NApi::EMasterChannelKind ReadFrom;
    //! Used only for ReadFrom == Cache.
    TDuration MasterCacheExpireTime;

    REGISTER_YSON_STRUCT(TDiscoveryV1Config);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryV1Config)

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryV2Config
    : public virtual TDiscoveryBaseConfig
    , public NDiscoveryClient::TDiscoveryClientConfig
    , public NDiscoveryClient::TMemberClientConfig
{
public:
    REGISTER_YSON_STRUCT(TDiscoveryV2Config);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryV2Config)

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryConfig
    : public TDiscoveryV1Config
    , public TDiscoveryV2Config
{
public:
    int Version;

    REGISTER_YSON_STRUCT(TDiscoveryConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
