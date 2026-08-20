#pragma once

#include "public.h"

#include <yt/yt/core/misc/arithmetic_formula.h>
#include <yt/yt/core/net/config.h>
#include <yt/yt/core/ytree/yson_struct.h>


namespace NYT::NYqlPlugin {

////////////////////////////////////////////////////////////////////////////////

using TQueryId = TGuid;

struct TVanillaJobFile
    : public NYTree::TYsonStruct
{
    TString Name;
    TString LocalPath;

    REGISTER_YSON_STRUCT(TVanillaJobFile);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TVanillaJobFile)

////////////////////////////////////////////////////////////////////////////////

struct TDQYTBackend
    : public NYTree::TYsonStruct
{
    TString ClusterName;
    TString ProxyAddress;
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
    bool UseLocalLDLibraryPath;
    TBooleanFormula SchedulingTagFilter;

    REGISTER_YSON_STRUCT(TDQYTBackend);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDQYTBackend)

////////////////////////////////////////////////////////////////////////////////

struct TDQYTCoordinator
    : public NYTree::TYsonStruct
{
    TString ClusterName;
    TString ProxyAddress;
    TString Prefix;
    TString TokenFile;
    TString User;
    TString DebugLogFile;

    REGISTER_YSON_STRUCT(TDQYTCoordinator);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDQYTCoordinator)

////////////////////////////////////////////////////////////////////////////////

struct TDQManagerConfig
    : public NYTree::TYsonStruct
{
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

struct TAdditionalSystemLib
    : public NYTree::TYsonStruct
{
    TString File;

    REGISTER_YSON_STRUCT(TAdditionalSystemLib);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAdditionalSystemLib)

////////////////////////////////////////////////////////////////////////////////

struct TProcessYqlPluginConfig
    : public NYTree::TYsonStruct
{
    bool Enabled;
    int SlotCount;
    TString SlotsRootPath;

    TDuration CheckProcessActiveDelay;

    TDuration DefaultRequestTimeout;
    TDuration RunRequestTimeout;

    NLogging::TLogManagerConfigPtr LogManagerTemplate;

    REGISTER_YSON_STRUCT(TProcessYqlPluginConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProcessYqlPluginConfig)

////////////////////////////////////////////////////////////////////////////////

struct TYqlPluginConfig
    : public NYTree::TYsonStruct
{
    //! Set default settings for NYql::TYtClusterConfig.
    static NYTree::IListNodePtr MergeClusterDefaultSettings(const NYTree::IListNodePtr& clusterConfigSettings);

    //! Fields from NYql::TYtGatewayConfig with snake case keys.
    NYTree::INodePtr GatewayConfig;

    //! Fields from NYql::TDQGatewayConfig with snake case keys.
    NYTree::INodePtr DQGatewayConfig;

    //! Fields from NYql::TYtflowGatewayConfig with snake case keys.
    NYTree::INodePtr YtflowGatewayConfig;

    //! Fields from NYql::PQGatewayConfig with snake case keys.
    NYTree::INodePtr PQGatewayConfig;

    //! Fields from NYql::SolomonGatewayConfig with snake case keys.
    NYTree::INodePtr SolomonGatewayConfig;

    //! Fields from NYT::NYqlPlugin::TDQManagerConfig with snake case keys.
    TDQManagerConfigPtr DQManagerConfig;

    bool EnableDQ;

    //! Fields from NYql::TFileStorageConfig with snake case keys.
    NYTree::INodePtr FileStorageConfig;

    //! Fields from NYql::TYtTvmConfig with snake case keys.
    NYTree::INodePtr TvmConfig;

    //! Fields from NYql::TYtAccessProviderConfig with snake case keys.
    NYTree::INodePtr YtAccessProviderConfig;

    NYTree::INodePtr OperationAttributes;

    THashMap<TString, TString> Libraries;

    TString YTTokenPath;

    std::vector<TAdditionalSystemLibPtr> AdditionalSystemLibs;

    TProcessYqlPluginConfigPtr ProcessPluginConfig;

    REGISTER_YSON_STRUCT(TYqlPluginConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlPluginConfig)

////////////////////////////////////////////////////////////////////////////////

struct TYqlPluginDynamicConfig
    : public NYTree::TYsonStruct
{
    NYson::TYsonString GatewaysConfig;
    TString MaxSupportedYqlVersion;

    THashMap<TString, TString> ProtoGatewaysConfigs;

    REGISTER_YSON_STRUCT(TYqlPluginDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlPluginDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
