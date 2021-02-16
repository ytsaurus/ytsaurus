#pragma once

#include "public.h"

#include <yt/server/lib/core_dump/config.h>

#include <yt/ytlib/chunk_client/config.h>

#include <yt/ytlib/program/config.h>

#include <yt/core/net/address.h>

#include <yt/core/rpc/config.h>

#include <yt/core/bus/tcp/config.h>

#include <yt/core/tracing/config.h>

#include <yt/core/logging/config.h>

#include <yt/core/http/config.h>

#include <yt/core/ytree/yson_serializable.h>

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

class TArchiveReporterConfig
    : public NYTree::TYsonSerializable
{
public:
    bool Enabled;
    TDuration ReportingPeriod;
    TDuration MinRepeatDelay;
    TDuration MaxRepeatDelay;
    int MaxInProgressDataSize;
    int MaxItemsInBatch;
    TString Path;

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
        RegisterParameter("max_in_progress_data_size", MaxInProgressDataSize)
            .Default(250_MB);
        RegisterParameter("max_items_in_batch", MaxItemsInBatch)
            .Default(1000);
        RegisterParameter("path", Path);
    }
};

DEFINE_REFCOUNTED_TYPE(TArchiveReporterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
