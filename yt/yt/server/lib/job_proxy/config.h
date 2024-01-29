#pragma once

#include "public.h"

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/lib/rpc_proxy/config.h>

#include <yt/yt/ytlib/auth/config.h>

#include <yt/yt/ytlib/hydra/config.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/job_proxy/public.h>
#include <yt/yt/ytlib/job_proxy/config.h>

#include <yt/yt/library/tracing/jaeger/tracer.h>

#include <yt/yt/client/file_client/config.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/dns_over_rpc/client/config.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TJobThrottlerConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration MinBackoffTime;
    TDuration MaxBackoffTime;
    double BackoffMultiplier;

    TDuration RpcTimeout;

    NConcurrency::TPrefetchingThrottlerConfigPtr BandwidthPrefetch;
    NConcurrency::TPrefetchingThrottlerConfigPtr RpsPrefetch;

    REGISTER_YSON_STRUCT(TJobThrottlerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobThrottlerConfig)

////////////////////////////////////////////////////////////////////////////////

class TCoreWatcherConfig
    : public NYTree::TYsonStruct
{
public:
    //! Cores lookup period.
    TDuration Period;

    //! Input/output operations timeout.
    TDuration IOTimeout;

    //! Finalization timeout.
    TDuration FinalizationTimeout;

    //! Cumulative timeout for cores processing.
    TDuration CoresProcessingTimeout;

    REGISTER_YSON_STRUCT(TCoreWatcherConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCoreWatcherConfig)

////////////////////////////////////////////////////////////////////////////////

class TUserJobNetworkAddress
    : public NYTree::TYsonStruct
{
public:
    NNet::TIP6Address Address;

    TString Name;

    REGISTER_YSON_STRUCT(TUserJobNetworkAddress);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserJobNetworkAddress)

////////////////////////////////////////////////////////////////////////////////

class TTmpfsManagerConfig
    : public NYTree::TYsonStruct
{
public:
    std::vector<TString> TmpfsPaths;

    REGISTER_YSON_STRUCT(TTmpfsManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTmpfsManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TMemoryTrackerConfig
    : public NYTree::TYsonStruct
{
public:
    bool IncludeMemoryMappedFiles;

    bool UseSMapsMemoryTracker;

    TDuration MemoryStatisticsCachePeriod;

    REGISTER_YSON_STRUCT(TMemoryTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMemoryTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

class TBindConfig
    : public NYTree::TYsonStruct
{
public:
    TString ExternalPath;
    TString InternalPath;
    bool ReadOnly;

    REGISTER_YSON_STRUCT(TBindConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBindConfig)

////////////////////////////////////////////////////////////////////////////////

class TJobProxyInternalConfig
    : public TNativeServerConfig
{
public:
    // Job-specific parameters.
    int SlotIndex = -1;

    TString SlotPath;

    TTmpfsManagerConfigPtr TmpfsManager;

    TMemoryTrackerConfigPtr MemoryTracker;

    //! Bind mounts for user job container.
    //! Includes "root_fs_binds" and artifacts if they are passed as bind mounts.
    std::vector<TBindConfigPtr> Binds;

    std::vector<int> GpuIndexes;

    //! Path for container root, if root volume is already prepared.
    std::optional<TString> RootPath;

    //! Docker image to build root volume as part of a container.
    std::optional<TString> DockerImage;

    bool MakeRootFSWritable;

    //! Path to write job proxy stderr (for testing purposes).
    std::optional<TString> StderrPath;
    //! Path to write executor stderr (for testing purposes).
    std::optional<TString> ExecutorStderrPath;

    NBus::TBusClientConfigPtr SupervisorConnection;
    TDuration SupervisorRpcTimeout;

    NBus::TBusClientConfigPtr TvmBridgeConnection;
    NAuth::TTvmBridgeConfigPtr TvmBridge;

    TDuration HeartbeatPeriod;
    TDuration InputPipeBlinkerPeriod;

    NYTree::INodePtr JobEnvironment;

    //! Addresses derived from node local descriptor to leverage locality.
    NNodeTrackerClient::TAddressMap Addresses;
    TString LocalHostName;
    std::optional<TString> Rack;
    std::optional<TString> DataCenter;

    i64 AheadMemoryReserve;

    bool AlwaysAbortOnMemoryReserveOverdraft;

    bool TestRootFS;

    TJobThrottlerConfigPtr JobThrottler;

    //! Hostname to set in container.
    std::optional<TString> HostName;

    bool EnableNat64;
    bool DisableNetwork;

    //! Network addresses to bind into container.
    std::vector<TUserJobNetworkAddressPtr> NetworkAddresses;

    bool AbortOnUnrecognizedOptions;

    bool AbortOnUncaughtException;

    TCoreWatcherConfigPtr CoreWatcher;

    bool TestPollJobShell;

    //! If set, user job will not receive uid.
    //! For testing purposes only.
    bool DoNotSetUserId;

    //! This option can disable memory limit check for user jobs.
    //! Used in arcadia tests, since it's almost impossible to set
    //! proper memory limits for asan builds.
    bool CheckUserJobMemoryLimit;

    //! Compat option for urgent disable of job shell audit.
    bool EnableJobShellSeccopm;

    //! Enabled using Porto kill for signalling instead of manual discovery of process pid.
    bool UsePortoKillForSignalling;

    bool ForceIdleCpuPolicy;

    bool SendHeartbeatBeforeAbort;

    //! If set, lightweight chunk specs for stderr and core tables
    //! will be sent in heartbeats to the controller agent.
    bool EnableStderrAndCoreLivePreview;

    std::optional<double> ContainerCpuLimit;

    std::optional<i64> SlotContainerMemoryLimit;

    NYT::NRpcProxy::TApiServiceConfigPtr ApiService;

    std::optional<int> StatisticsOutputTableCountLimit;

    NDns::TDnsOverRpcResolverConfigPtr DnsOverRpcResolver;

    NJobProxy::TJobTestingOptionsPtr JobTestingOptions;

    NAuth::TAuthenticationManagerConfigPtr AuthenticationManager;

    //! Supports ability to use direct connection to masters.
    NApi::NNative::TConnectionCompoundConfigPtr OriginalClusterConnection;

    REGISTER_YSON_STRUCT(TJobProxyInternalConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobProxyInternalConfig)

////////////////////////////////////////////////////////////////////////////////

class TJobProxyDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    NTracing::TJaegerTracerDynamicConfigPtr Jaeger;

    bool EnableJobShellSeccopm;

    bool UsePortoKillForSignalling;

    bool ForceIdleCpuPolicy;

    bool AbortOnUncaughtException;

    bool EnableStderrAndCoreLivePreview;

    NYTree::INodePtr JobEnvironment;

    REGISTER_YSON_STRUCT(TJobProxyDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobProxyDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
