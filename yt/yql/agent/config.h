#pragma once

#include "private.h"

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/dynamic_config/config.h>

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

class TDqYtBackend
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

    REGISTER_YSON_STRUCT(TDqYtBackend);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDqYtBackend)

class TDqYtCoordinator
    : public NYTree::TYsonStruct
{
public:
    TString ClusterName;
    TString Prefix;
    TString TokenFile;
    TString User;
    TString DebugLogFile;

    REGISTER_YSON_STRUCT(TDqYtCoordinator);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDqYtCoordinator)

class TDqManagerConfig
    : public NYTree::TYsonStruct
{
public:
    ui16 InterconnectPort;
    ui16 GrpcPort;
    ui32 ActorThreads;
    bool UseIPv4;
    std::vector<TDqYtBackendPtr> YtBackends;
    TDqYtCoordinatorPtr YtCoordinator;

    //! Dq Interconnect Settings. Fields from NYql::NProto::TDqConfig::TICSettings with snake case keys.
    NYTree::INodePtr ICSettings;

    REGISTER_YSON_STRUCT(TDqManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDqManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TYqlPluginConfig
    : public NYTree::TYsonStruct
{
public:
    //! Set default settings for NYql::TYtClusterConfig.
    static NYTree::IListNodePtr MergeClusterDefaultSettings(const NYTree::IListNodePtr& clusterConfigSettings);

    //! Fields from NYql::TYtGatewayConfig with snake case keys.
    NYTree::INodePtr GatewayConfig;

    //! Fields from NYql::TDqGatewayConfig with snake case keys.
    NYTree::INodePtr DqGatewayConfig;

    //! Fields from NYT::NYqlPlugin::TDqManagerConfig with snake case keys.
    TDqManagerConfigPtr DqManagerConfig;

    bool EnableDq;

    //! Fields from NYql::TFileStorageConfig with snake case keys.
    NYTree::INodePtr FileStorageConfig;

    NYTree::INodePtr OperationAttributes;

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
    REGISTER_YSON_STRUCT(TYqlAgentDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlAgentDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TYqlAgentServerConfig
    : public TNativeServerConfig
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
    : public TNativeSingletonsDynamicConfig
{
public:
    TYqlAgentDynamicConfigPtr YqlAgent;

    REGISTER_YSON_STRUCT(TYqlAgentServerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlAgentServerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
