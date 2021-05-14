#pragma once

#include "public.h"

#include <yt/yt/server/lib/core_dump/config.h>

#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/ytlib/program/config.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/logging/config.h>

#include <yt/yt/core/http/config.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TServerConfig
    : public TSingletonsConfig
    , public TDiagnosticDumpConfig
{
public:
    NBus::TTcpBusServerConfigPtr BusServer;
    NRpc::TServerConfigPtr RpcServer;
    NCoreDump::TCoreDumperConfigPtr CoreDumper;

    int RpcPort;
    int MonitoringPort;

    TServerConfig();

    NHttp::TServerConfigPtr CreateMonitoringHttpServerConfig();
};

////////////////////////////////////////////////////////////////////////////////

class TDiskLocationConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Root directory for the location.
    TString Path;

    //! Minimum size the disk partition must have to make this location usable.
    std::optional<i64> MinDiskSpace;

    TDiskLocationConfig();
};

DEFINE_REFCOUNTED_TYPE(TDiskLocationConfig)

////////////////////////////////////////////////////////////////////////////////

class TDiskHealthCheckerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Period between consequent checks.
    TDuration CheckPeriod;

    //! Size of the test file.
    i64 TestSize;

    //! Maximum time allowed for execution of a single check.
    TDuration Timeout;

    TDiskHealthCheckerConfig();
};

DEFINE_REFCOUNTED_TYPE(TDiskHealthCheckerConfig)

////////////////////////////////////////////////////////////////////////////////

class TFormatConfigBase
    : public NYTree::TYsonSerializable
{
public:
    bool Enable;
    NYTree::IMapNodePtr DefaultAttributes;

    TFormatConfigBase();
};

DEFINE_REFCOUNTED_TYPE(TFormatConfigBase)

class TFormatConfig
    : public TFormatConfigBase
{
public:
    THashMap<TString, TFormatConfigBasePtr> UserOverrides;

    TFormatConfig();
};

DEFINE_REFCOUNTED_TYPE(TFormatConfig)

////////////////////////////////////////////////////////////////////////////////

//! Part of the ArchiveReporter configuration with common options.
//! Options which are supposed to be changed independently for every archive table
//! are listed in TArchiveHandlerConfig.
class TArchiveReporterConfig
    : public NYTree::TYsonSerializable
{
public:
    bool Enabled;
    TDuration ReportingPeriod;
    TDuration MinRepeatDelay;
    TDuration MaxRepeatDelay;
    int MaxItemsInBatch;

    TArchiveReporterConfig()
    {
        RegisterParameter("enabled", Enabled)
            .Default(true);
        RegisterParameter("reporting_period", ReportingPeriod)
            .Default(TDuration::Seconds(5));
        RegisterParameter("min_repeat_delay", MinRepeatDelay)
            .Default(TDuration::Seconds(10));
        RegisterParameter("max_repeat_delay", MaxRepeatDelay)
            .Default(TDuration::Minutes(5));
        RegisterParameter("max_items_in_batch", MaxItemsInBatch)
            .Default(1000);
    }
};

DEFINE_REFCOUNTED_TYPE(TArchiveReporterConfig)

////////////////////////////////////////////////////////////////////////////////

//! Part of the ArchiveReporter configuration with unique per-table options.
class TArchiveHandlerConfig
    : public NYTree::TYsonSerializable
{
public:
    int MaxInProgressDataSize;
    TString Path;

    TArchiveHandlerConfig()
    {
        RegisterParameter("max_in_progress_data_size", MaxInProgressDataSize)
            .Default(250_MB);
        RegisterParameter("path", Path)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TArchiveHandlerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
