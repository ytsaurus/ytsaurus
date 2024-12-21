#pragma once

#include "private.h"

#include <yt/yt/server/lib/cypress_election/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/http/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/dynamic_config/config.h>

#include <yt/yt/library/server_program/config.h>

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

class TEngineConfigBase
    : public NYTree::TYsonStruct
{
public:
    TDuration QueryStateWriteBackoff;
    TDuration QueryProgressWritePeriod;
    i64 RowCountLimit;

    // Resulting rowset is passed via wire protocol.
    // Wire protocol reader validates that rowset doesn't contain values above this limit.
    i64 ResultingRowsetValueLengthLimit;

    REGISTER_YSON_STRUCT(TEngineConfigBase);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TEngineConfigBase)

////////////////////////////////////////////////////////////////////////////////

class TYqlEngineConfig
    : public TEngineConfigBase
{
public:
    TString Stage;
    TDuration QueryProgressGetPeriod;
    TDuration StartQueryAttemptPeriod;

    REGISTER_YSON_STRUCT(TYqlEngineConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlEngineConfig)

////////////////////////////////////////////////////////////////////////////////

class TChytEngineConfig
    : public TEngineConfigBase
{
public:
    TString DefaultClique;
    TString DefaultCluster;
    TDuration ProgressPollPeriod;

    REGISTER_YSON_STRUCT(TChytEngineConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChytEngineConfig)

////////////////////////////////////////////////////////////////////////////////

class TQLEngineConfig
    : public TEngineConfigBase
{
public:
    TString DefaultCluster;

    REGISTER_YSON_STRUCT(TQLEngineConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQLEngineConfig)

////////////////////////////////////////////////////////////////////////////////

class TSpytEngineConfig
    : public TEngineConfigBase
{
public:
    TString DefaultCluster;
    NYPath::TYPath DefaultDiscoveryPath;
    NYPath::TYPath DefaultDiscoveryGroup;
    NYPath::TYPath SpytHome;
    NHttp::TClientConfigPtr HttpClient;
    TDuration StatusPollPeriod;

    TDuration TokenExpirationTimeout;
    TDuration RefreshTokenPeriod;

    REGISTER_YSON_STRUCT(TSpytEngineConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSpytEngineConfig)

////////////////////////////////////////////////////////////////////////////////

class TQueryTrackerProxyConfig
    : public NYTree::TYsonStruct
{
public:
    i64 MaxQueryFileCount;
    i64 MaxQueryFileNameSizeBytes;
    i64 MaxQueryFileContentSizeBytes;

    REGISTER_YSON_STRUCT(TQueryTrackerProxyConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryTrackerProxyConfig)

////////////////////////////////////////////////////////////////////////////////

class TQueryTrackerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration StateCheckPeriod;

    TDuration ActiveQueryAcquisitionPeriod;
    TDuration ActiveQueryPingPeriod;
    TDuration QueryFinishBackoff;
    TDuration HealthCheckPeriod;

    TEngineConfigBasePtr MockEngine;
    TQLEngineConfigPtr QLEngine;
    TYqlEngineConfigPtr YqlEngine;
    TChytEngineConfigPtr ChytEngine;
    TSpytEngineConfigPtr SpytEngine;

    TQueryTrackerProxyConfigPtr ProxyConfig;

    REGISTER_YSON_STRUCT(TQueryTrackerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryTrackerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TQueryTrackerServerConfig
    : public TNativeServerConfig
    , public TServerProgramConfig
{
public:
    int MinRequiredStateVersion;
    bool AbortOnUnrecognizedOptions;

    int ProxyThreadPoolSize;

    std::string User;

    NYTree::IMapNodePtr CypressAnnotations;

    NCypressElection::TCypressElectionManagerConfigPtr ElectionManager;

    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;
    TString DynamicConfigPath;

    TString Root;

    REGISTER_YSON_STRUCT(TQueryTrackerServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryTrackerServerConfig)

////////////////////////////////////////////////////////////////////////////////

class TQueryTrackerServerDynamicConfig
    : public TSingletonsDynamicConfig
{
public:
    NAlertManager::TAlertManagerDynamicConfigPtr AlertManager;
    TQueryTrackerDynamicConfigPtr QueryTracker;

    REGISTER_YSON_STRUCT(TQueryTrackerServerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryTrackerServerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
