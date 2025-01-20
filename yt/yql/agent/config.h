#pragma once

#include "private.h"

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/net/config.h>

#include <yt/yt/library/dynamic_config/config.h>

#include <yt/yt/library/server_program/config.h>

namespace NYT::NYqlAgent {

////////////////////////////////////////////////////////////////////////////////

class TVanillaJobFile
    : public NYTree::TYsonStruct
{
public:
    TString Name;
    TString LocalPath;

    REGISTER_YSON_STRUCT(TVanillaJobFile);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TVanillaJobFile)

class TDQYTBackend
    : public NYTree::TYsonStruct
{
public:
    TString ClusterName;
    ui32 JobsPerOperation;
    ui32 MaxJobs;
    TString VanillaJobLite;
    TString VanillaJobCommand;
    std::vector<TVanillaJobFilePtr> VanillaJobFiles;
    TString Prefix;
    ui32 UploadReplicationFactor;
    TString TokenFile;
    TString User;
    TString Pool;
    std::vector<TString> PoolTrees;
    std::vector<TString> Owner;
    i64 CpuLimit;
    i32 WorkerCapacity;
    i64 MemoryLimit;
    i64 CacheSize;
    bool UseTmpFs;
    TString NetworkProject;
    bool CanUseComputeActor;
    bool EnforceJobUtc;

    REGISTER_YSON_STRUCT(TDQYTBackend);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDQYTBackend)

class TDQYTCoordinator
    : public NYTree::TYsonStruct
{
public:
    TString ClusterName;
    TString Prefix;
    TString TokenFile;
    TString User;
    TString DebugLogFile;

    REGISTER_YSON_STRUCT(TDQYTCoordinator);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDQYTCoordinator)

class TDQManagerConfig
    : public NYTree::TYsonStruct
{
public:
    ui16 InterconnectPort;
    ui16 GrpcPort;
    ui32 ActorThreads;
    bool UseIPv4;
    std::vector<TDQYTBackendPtr> YTBackends;
    TDQYTCoordinatorPtr YTCoordinator;

    //! Address resolver used in DQ operation. Is taken from singletons config if not set.
    NNet::TAddressResolverConfigPtr AddressResolver;

    //! DQ Interconnect Settings. Fields from NYql::NProto::TDqConfig::TICSettings with snake case keys.
    NYTree::INodePtr ICSettings;

    REGISTER_YSON_STRUCT(TDQManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDQManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TYqlPluginConfig
    : public NYTree::TYsonStruct
{
public:
    //! Set default settings for NYql::TYtClusterConfig.
    static NYTree::IListNodePtr MergeClusterDefaultSettings(const NYTree::IListNodePtr& clusterConfigSettings);

    //! Fields from NYql::TYtGatewayConfig with snake case keys.
    NYTree::INodePtr GatewayConfig;

    //! Fields from NYql::TDQGatewayConfig with snake case keys.
    NYTree::INodePtr DQGatewayConfig;

    //! Fields from NYT::NYqlPlugin::TDQManagerConfig with snake case keys.
    TDQManagerConfigPtr DQManagerConfig;

    bool EnableDQ;

    //! Fields from NYql::TFileStorageConfig with snake case keys.
    NYTree::INodePtr FileStorageConfig;

    NYTree::INodePtr OperationAttributes;

    THashMap<TString, TString> Libraries;

    TString YTTokenPath;

    //! Path to libyqlplugin.so. Ignored when built with -DYQL_NATIVE=yes.
    std::optional<TString> YqlPluginSharedLibrary;

    REGISTER_YSON_STRUCT(TYqlPluginConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlPluginConfig)

////////////////////////////////////////////////////////////////////////////////

class TYqlAgentConfig
    : public TYqlPluginConfig
{
public:
    TDuration TokenExpirationTimeout;
    TDuration RefreshTokenPeriod;
    int IssueTokenAttempts;

    //! Used to create channels to other queue agents.
    NBus::TBusConfigPtr BusClient;

    int YqlThreadCount;

    REGISTER_YSON_STRUCT(TYqlAgentConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlAgentConfig)

////////////////////////////////////////////////////////////////////////////////

class TYqlAgentDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    int MaxSimultaneousQueries;
    TDuration StateCheckPeriod;

    REGISTER_YSON_STRUCT(TYqlAgentDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlAgentDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TYqlAgentServerConfig
    : public NServer::TNativeServerBootstrapConfig
    , public TServerProgramConfig
{
public:
    TYqlAgentConfigPtr YqlAgent;

    bool AbortOnUnrecognizedOptions;

    //! User for native client.
    TString User;

    //! The path of directory for orchids.
    NYPath::TYPath Root;

    NYTree::IMapNodePtr CypressAnnotations;

    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;
    TString DynamicConfigPath;

    REGISTER_YSON_STRUCT(TYqlAgentServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlAgentServerConfig)

////////////////////////////////////////////////////////////////////////////////

class TYqlAgentServerDynamicConfig
    : public TSingletonsDynamicConfig
{
public:
    TYqlAgentDynamicConfigPtr YqlAgent;

    REGISTER_YSON_STRUCT(TYqlAgentServerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlAgentServerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
