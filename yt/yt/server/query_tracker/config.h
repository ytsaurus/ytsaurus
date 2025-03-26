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

struct TYqlEngineConfig
    : public TEngineConfigBase
{
    TString Stage;
    TDuration QueryProgressGetPeriod;
    TDuration StartQueryAttemptPeriod;

    REGISTER_YSON_STRUCT(TYqlEngineConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlEngineConfig)

////////////////////////////////////////////////////////////////////////////////

struct TChytEngineConfig
    : public TEngineConfigBase
{
    TString DefaultClique;
    std::string DefaultCluster;
    TDuration ProgressPollPeriod;

    REGISTER_YSON_STRUCT(TChytEngineConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChytEngineConfig)

////////////////////////////////////////////////////////////////////////////////

struct TQLEngineConfig
    : public TEngineConfigBase
{
    std::string DefaultCluster;

    REGISTER_YSON_STRUCT(TQLEngineConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQLEngineConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSpytEngineConfig
    : public TEngineConfigBase
{
    std::string DefaultCluster;
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

struct TQueryTrackerProxyConfig
    : public NYTree::TYsonStruct
{
    i64 MaxQueryFileCount;
    i64 MaxQueryFileNameSizeBytes;
    i64 MaxQueryFileContentSizeBytes;

    REGISTER_YSON_STRUCT(TQueryTrackerProxyConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryTrackerProxyConfig)

////////////////////////////////////////////////////////////////////////////////

struct TQueryTrackerDynamicConfig
    : public NYTree::TYsonStruct
{
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

struct TQueryTrackerBootstrapConfig
    : public NServer::TNativeServerBootstrapConfig
{
    int MinRequiredStateVersion;
    bool AbortOnUnrecognizedOptions;

    int ProxyThreadPoolSize;

    std::string User;

    NYTree::IMapNodePtr CypressAnnotations;

    NCypressElection::TCypressElectionManagerConfigPtr ElectionManager;

    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;
    TString DynamicConfigPath;

    TString Root;

    REGISTER_YSON_STRUCT(TQueryTrackerBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryTrackerBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

struct TQueryTrackerProgramConfig
    : public TQueryTrackerBootstrapConfig
    , public TServerProgramConfig
{
    REGISTER_YSON_STRUCT(TQueryTrackerProgramConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryTrackerProgramConfig)

////////////////////////////////////////////////////////////////////////////////

struct TQueryTrackerComponentDynamicConfig
    : public TSingletonsDynamicConfig
{
    NAlertManager::TAlertManagerDynamicConfigPtr AlertManager;
    TQueryTrackerDynamicConfigPtr QueryTracker;

    REGISTER_YSON_STRUCT(TQueryTrackerComponentDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryTrackerComponentDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
