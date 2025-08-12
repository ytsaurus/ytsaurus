#pragma once

#include "public.h"

#include <yt/yt/core/misc/arithmetic_formula.h>
#include <yt/yt/core/net/config.h>
#include <yt/yt/core/ytree/yson_struct.h>


namespace NYT::NYqlPlugin {

////////////////////////////////////////////////////////////////////////////////

using TQueryId = TGuid;

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
    bool UseLocalLDLibraryPath;
    TBooleanFormula SchedulingTagFilter;

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

struct TYqlProcessPluginConfig
    : public NYTree::TYsonStruct
{
    bool Enabled;
    int SlotsCount;
    TString SlotsRootPath;

    TDuration CheckProcessActiveDelay;

    NLogging::TLogManagerConfigPtr LogManagerTemplate;

    REGISTER_YSON_STRUCT(TYqlProcessPluginConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlProcessPluginConfig)

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

    //! Fields from NYT::NYqlPlugin::TDQManagerConfig with snake case keys.
    TDQManagerConfigPtr DQManagerConfig;

    bool EnableDQ;

    //! Fields from NYql::TFileStorageConfig with snake case keys.
    NYTree::INodePtr FileStorageConfig;

    NYTree::INodePtr OperationAttributes;

    THashMap<TString, TString> Libraries;

    TString YTTokenPath;

    TString UIOrigin;

    //! Path to libyqlplugin.so. Ignored when built with -DYQL_NATIVE=yes.
    std::optional<TString> YqlPluginSharedLibrary;

    std::vector<TAdditionalSystemLibPtr> AdditionalSystemLibs;

    TYqlProcessPluginConfigPtr ProcessPluginConfig;

    REGISTER_YSON_STRUCT(TYqlPluginConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlPluginConfig)

////////////////////////////////////////////////////////////////////////////////

}