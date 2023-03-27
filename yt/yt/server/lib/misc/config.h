#pragma once

#include "public.h"

#include <yt/yt/library/coredumper/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/program/config.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/bus/tcp/public.h>

#include <yt/yt/core/logging/public.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A configuration for a server which does not necessarily have explicitly defined
//! "native" cluster. Examples of non-native components are timestamp providers
//! and discovery servers.
class TServerConfig
    : public TDiagnosticDumpConfig
    , public virtual TSingletonsConfig
{
public:
    NBus::TBusServerConfigPtr BusServer;
    NRpc::TServerConfigPtr RpcServer;
    NCoreDump::TCoreDumperConfigPtr CoreDumper;

    int RpcPort;
    int TvmOnlyRpcPort;
    int MonitoringPort;

    NHttp::TServerConfigPtr CreateMonitoringHttpServerConfig();

    REGISTER_YSON_STRUCT(TServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServerConfig)

////////////////////////////////////////////////////////////////////////////////

class TNativeServerConfig
    : public TServerConfig
    , public TNativeSingletonsConfig
{
public:
    NApi::NNative::TConnectionCompoundConfigPtr ClusterConnection;

    NApi::NNative::EClusterConnectionDynamicConfigPolicy ClusterConnectionDynamicConfigPolicy;

    REGISTER_YSON_STRUCT(TNativeServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNativeServerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDiskLocationConfig
    : public virtual NYTree::TYsonStruct
{
public:
    //! Root directory for the location.
    TString Path;

    //! Minimum size the disk partition must have to make this location usable.
    std::optional<i64> MinDiskSpace;

    //! Block device name.
    TString DeviceName;

    //! Storage device vendor info.
    TString DeviceModel;

    //! Disk family in this location (HDD, SDD, etc.)
    TString DiskFamily;

    void ApplyDynamicInplace(const TDiskLocationDynamicConfig& dynamicConfig);

    REGISTER_YSON_STRUCT(TDiskLocationConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiskLocationConfig)

////////////////////////////////////////////////////////////////////////////////

class TDiskLocationDynamicConfig
    : public virtual NYTree::TYsonStruct
{
public:
    std::optional<i64> MinDiskSpace;

    REGISTER_YSON_STRUCT(TDiskLocationDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiskLocationDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TDiskHealthCheckerConfig
    : public NYTree::TYsonStruct
{
public:
    //! Period between consequent checks.
    TDuration CheckPeriod;

    //! Size of the test file.
    i64 TestSize;

    //! Maximum time allowed for execution of a single check.
    TDuration Timeout;

    REGISTER_YSON_STRUCT(TDiskHealthCheckerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiskHealthCheckerConfig)

////////////////////////////////////////////////////////////////////////////////

class TFormatConfigBase
    : public NYTree::TYsonStruct
{
public:
    bool Enable;
    NYTree::IMapNodePtr DefaultAttributes;

    REGISTER_YSON_STRUCT(TFormatConfigBase);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFormatConfigBase)

////////////////////////////////////////////////////////////////////////////////

class TFormatConfig
    : public TFormatConfigBase
{
public:
    THashMap<TString, TFormatConfigBasePtr> UserOverrides;

    REGISTER_YSON_STRUCT(TFormatConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFormatConfig)

////////////////////////////////////////////////////////////////////////////////

//! Part of the ArchiveReporter configuration with common options.
//! Options which are supposed to be changed independently for every archive table
//! are listed in TArchiveHandlerConfig.
class TArchiveReporterConfig
    : public NYTree::TYsonStruct
{
public:
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
class TArchiveHandlerConfig
    : public NYTree::TYsonStruct
{
public:
    i64 MaxInProgressDataSize;
    TString Path;

    REGISTER_YSON_STRUCT(TArchiveHandlerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TArchiveHandlerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
