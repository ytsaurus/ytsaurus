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

class TCypressProxyBootstrapConfig
    : public NServer::TNativeServerBootstrapConfig
{
public:
    bool AbortOnUnrecognizedOptions;

    TCypressRegistrarConfigPtr CypressRegistrar;

    NYPath::TYPath RootPath;

    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;
    NYPath::TYPath DynamicConfigPath;

    TUserDirectorySynchronizerConfigPtr UserDirectorySynchronizer;

    REGISTER_YSON_STRUCT(TCypressProxyBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressProxyBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

class TCypressProxyProgramConfig
    : public TCypressProxyBootstrapConfig
    , public TServerProgramConfig
{
public:
    REGISTER_YSON_STRUCT(TCypressProxyProgramConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressProxyProgramConfig)

////////////////////////////////////////////////////////////////////////////////

class TUserDirectorySynchronizerConfig
    : public NYTree::TYsonStruct
{
public:
    //! Interval between consequent directory updates.
    TDuration SyncPeriod;

    TDuration SyncSplay;

    REGISTER_YSON_STRUCT(TUserDirectorySynchronizerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

class TObjectServiceDynamicConfig
    : public NYTree::TYsonStruct
{
public:
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

    REGISTER_YSON_STRUCT(TObjectServiceDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TObjectServiceDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TSequoiaResponseKeeperDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    //! If set to false, writing requests to Sequoia cannot be idempotent.
    bool Enable;

    REGISTER_YSON_STRUCT(TSequoiaResponseKeeperDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSequoiaResponseKeeperDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TCypressProxyDynamicConfig
    : public TSingletonsDynamicConfig
{
public:
    TObjectServiceDynamicConfigPtr ObjectService;

    TSequoiaResponseKeeperDynamicConfigPtr ResponseKeeper;

    REGISTER_YSON_STRUCT(TCypressProxyDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressProxyDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
