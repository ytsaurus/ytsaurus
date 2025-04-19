#pragma once

#include "public.h"

#include <yt/yt/server/lib/cypress_registrar/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/distributed_throttler/config.h>

#include <yt/yt/library/dynamic_config/config.h>

#include <yt/yt/library/server_program/config.h>

#include <yt/yt/core/bus/tcp/config.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

struct TCypressProxyBootstrapConfig
    : public NServer::TNativeServerBootstrapConfig
{
    bool AbortOnUnrecognizedOptions;

    TUserDirectorySynchronizerConfigPtr UserDirectorySynchronizer;

    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;

    TDuration HeartbeatPeriod;

    REGISTER_YSON_STRUCT(TCypressProxyBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressProxyBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCypressProxyProgramConfig
    : public TCypressProxyBootstrapConfig
    , public TServerProgramConfig
{
    REGISTER_YSON_STRUCT(TCypressProxyProgramConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressProxyProgramConfig)

////////////////////////////////////////////////////////////////////////////////

struct TUserDirectorySynchronizerConfig
    : public NYTree::TYsonStruct
{
    //! Interval between consequent directory updates.
    TDuration SyncPeriod;

    TDuration SyncSplay;

    REGISTER_YSON_STRUCT(TUserDirectorySynchronizerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TObjectServiceDynamicConfig
    : public NYTree::TYsonStruct
{
    //! Size of the thread pool used for object service requests execution.
    int ThreadPoolSize;

    //! Skip the first phase in the two-phase request execution at master.
    //! When set to |true|, all requests are resolved at Sequoia first.
    bool AllowBypassMasterResolve;

    // COMPAT(danilalexeev)
    bool AlertOnMixedReadWriteBatch;

    NDistributedThrottler::TDistributedThrottlerConfigPtr DistributedThrottler;

    bool EnablePerUserRequestWeightThrottling;

    NConcurrency::TThroughputThrottlerConfigPtr DefaultPerUserReadRequestWeightThrottler;
    NConcurrency::TThroughputThrottlerConfigPtr DefaultPerUserWriteRequestWeightThrottler;

    TDuration ForwardedRequestTimeoutReserve;

    // For testing purposes.
    bool EnableFastPathPrerequisiteTransactionCheck;

    REGISTER_YSON_STRUCT(TObjectServiceDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TObjectServiceDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSequoiaResponseKeeperDynamicConfig
    : public NYTree::TYsonStruct
{
    //! If set to false, writing requests to Sequoia cannot be idempotent.
    bool Enable;

    REGISTER_YSON_STRUCT(TSequoiaResponseKeeperDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSequoiaResponseKeeperDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCypressProxyDynamicConfig
    : public TSingletonsDynamicConfig
{
    TObjectServiceDynamicConfigPtr ObjectService;

    TSequoiaResponseKeeperDynamicConfigPtr ResponseKeeper;

    REGISTER_YSON_STRUCT(TCypressProxyDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressProxyDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
