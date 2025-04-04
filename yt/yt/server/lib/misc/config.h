#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/bus/tcp/public.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NServer {

////////////////////////////////////////////////////////////////////////////////

//! A configuration for a server which does not necessarily have explicitly defined
//! "native" cluster. Examples of non-native components are timestamp providers
//! and discovery servers.
struct TServerBootstrapConfig
    : public virtual NYTree::TYsonStruct
{
    NBus::TBusServerConfigPtr BusServer;
    NRpc::TServerConfigPtr RpcServer;

    int RpcPort;
    int TvmOnlyRpcPort;
    int MonitoringPort;
    //! This option may be used to prevent config-containing nodes to be exposed in Orchid as a mean of security
    //! (disclosing less information about YT servers to a potential attacker).
    bool ExposeConfigInOrchid;

    NHttp::TServerConfigPtr CreateMonitoringHttpServerConfig();

    REGISTER_YSON_STRUCT(TServerBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServerBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

struct TNativeServerBootstrapConfig
    : public NServer::TServerBootstrapConfig
{
    NApi::NNative::TConnectionCompoundConfigPtr ClusterConnection;
    NApi::NNative::EClusterConnectionDynamicConfigPolicy ClusterConnectionDynamicConfigPolicy;

    REGISTER_YSON_STRUCT(TNativeServerBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNativeServerBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDiskLocationConfig
    : public virtual NYTree::TYsonStruct
{
    //! Root directory for the location.
    TString Path;

    //! Minimum size the disk partition must have to make this location usable.
    std::optional<i64> MinDiskSpace;

    //! Block device name.
    TString DeviceName;
    static inline const TString UnknownDeviceName = "UNKNOWN";

    //! Storage device vendor info.
    TString DeviceModel;
    static inline const TString UnknownDeviceModel = "UNKNOWN";

    //! Disk family in this location (HDD, SDD, etc.)
    TString DiskFamily;
    static inline const TString UnknownDiskFamily = "UNKNOWN";

    void ApplyDynamicInplace(const TDiskLocationDynamicConfig& dynamicConfig);

    REGISTER_YSON_STRUCT(TDiskLocationConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiskLocationConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDiskLocationDynamicConfig
    : public virtual NYTree::TYsonStruct
{
    std::optional<i64> MinDiskSpace;

    REGISTER_YSON_STRUCT(TDiskLocationDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiskLocationDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDiskHealthCheckerConfig
    : public NYTree::TYsonStruct
{
    //! Period between consequent checks.
    TDuration CheckPeriod;

    //! Size of the test file.
    i64 TestSize;

    //! Maximum time allowed for execution of a single check.
    TDuration ExecTimeout;

    //! Maximum time allowed for waiting for execution.
    TDuration WaitTimeout;

    REGISTER_YSON_STRUCT(TDiskHealthCheckerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiskHealthCheckerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDiskHealthCheckerDynamicConfig
    : public virtual NYTree::TYsonStruct
{
    //! Size of the test file.
    std::optional<i64> TestSize;

    //! Maximum time allowed for execution of a single check.
    std::optional<TDuration> ExecTimeout;

    //! Maximum time allowed for waiting for execution.
    std::optional<TDuration> WaitTimeout;

    REGISTER_YSON_STRUCT(TDiskHealthCheckerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiskHealthCheckerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TFormatConfigBase
    : public NYTree::TYsonStruct
{
    bool Enable;
    NYTree::IMapNodePtr DefaultAttributes;

    REGISTER_YSON_STRUCT(TFormatConfigBase);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFormatConfigBase)

////////////////////////////////////////////////////////////////////////////////

struct TFormatConfig
    : public TFormatConfigBase
{
    THashMap<std::string, TFormatConfigBasePtr> UserOverrides;

    REGISTER_YSON_STRUCT(TFormatConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFormatConfig)

////////////////////////////////////////////////////////////////////////////////

//! Part of the ArchiveReporter configuration with common options.
//! Options which are supposed to be changed independently for every archive table
//! are listed in TArchiveHandlerConfig.
struct TArchiveReporterConfig
    : public NYTree::TYsonStruct
{
    bool Enabled;
    TDuration ReportingPeriod;
    TDuration MinRepeatDelay;
    TDuration MaxRepeatDelay;
    int MaxItemsInBatch;

    REGISTER_YSON_STRUCT(TArchiveReporterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TArchiveReporterConfig)

////////////////////////////////////////////////////////////////////////////////

//! Part of the ArchiveReporter configuration with unique per-table options.
struct TArchiveHandlerConfig
    : public NYTree::TYsonStruct
{
    i64 MaxInProgressDataSize;
    TString Path;

    REGISTER_YSON_STRUCT(TArchiveHandlerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TArchiveHandlerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TJobReporterConfig
    : public TArchiveReporterConfig
{
    TArchiveHandlerConfigPtr JobHandler;
    TArchiveHandlerConfigPtr OperationIdHandler;
    TArchiveHandlerConfigPtr JobSpecHandler;
    TArchiveHandlerConfigPtr JobStderrHandler;
    TArchiveHandlerConfigPtr JobFailContextHandler;
    TArchiveHandlerConfigPtr JobProfileHandler;
    TArchiveHandlerConfigPtr JobTraceEventHandler;

    std::string User;
    bool ReportStatisticsLz4;

    // COMPAT(dakovalkov): Delete these when all job reporter configs are in new format.
    std::optional<int> MaxInProgressJobDataSize;
    std::optional<int> MaxInProgressOperationIdDataSize;
    std::optional<int> MaxInProgressJobSpecDataSize;
    std::optional<int> MaxInProgressJobStderrDataSize;
    std::optional<int> MaxInProgressJobFailContextDataSize;

    // Enables job reporter to send job events/statistics etc.
    bool EnableJobReporter;

    // Enables job reporter to send job specs.
    bool EnableJobSpecReporter;

    // Enables job reporter to send job stderrs.
    bool EnableJobStderrReporter;

    // Enables job reporter to send job profiles.
    bool EnableJobProfileReporter;

    // Enables job reporter to send job fail contexts.
    bool EnableJobFailContextReporter;

    REGISTER_YSON_STRUCT(TJobReporterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobReporterConfig)

////////////////////////////////////////////////////////////////////////////////

struct THeapProfilerTestingOptions
    : public NYTree::TYsonStruct
{
    std::optional<i64> AllocationSize;
    std::optional<TDuration> AllocationReleaseDelay;

    REGISTER_YSON_STRUCT(THeapProfilerTestingOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THeapProfilerTestingOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServer
