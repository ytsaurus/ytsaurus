#pragma once

#include "public.h"

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NLogTailer {

////////////////////////////////////////////////////////////////////////////////

class TLogRotationConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enable;
    TDuration RotationPeriod;
    int LogSegmentCount;

    //! Pid of the process writing logs.
    //! During the log rotation SIGHUP will be sent to it.
    std::optional<int> LogWriterPid;

    //! Delay between SIGHUP and last read of old log.
    TDuration RotationDelay;

    REGISTER_YSON_STRUCT(TLogRotationConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("enable", &TThis::Enable)
            .Default(false);
        registrar.Parameter("rotation_period", &TThis::RotationPeriod)
            .Default();
        registrar.Parameter("log_segment_count", &TThis::LogSegmentCount)
            .Default(5);
        registrar.Parameter("log_writer_pid", &TThis::LogWriterPid)
            .Default();
        registrar.Parameter("rotation_delay", &TThis::RotationDelay)
            .Default(TDuration::MilliSeconds(50));
    }
};

DEFINE_REFCOUNTED_TYPE(TLogRotationConfig)

////////////////////////////////////////////////////////////////////////////////

class TLogTableConfig
    : public NYTree::TYsonStruct
{
public:
    NYTree::TYPath Path;
    bool RequireTraceId;

    REGISTER_YSON_STRUCT(TLogTableConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("path", &TThis::Path);
        registrar.Parameter("require_trace_id", &TThis::RequireTraceId)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TLogTableConfig)

/////////////////////////////////////////////////////////////////////////////

class TLogFileConfig
    : public NYTree::TYsonStruct
{
public:
    TString Path;
    std::vector<TLogTableConfigPtr> Tables;

    REGISTER_YSON_STRUCT(TLogFileConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("path", &TThis::Path)
            .Default();
        registrar.Parameter("tables", &TThis::Tables)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TLogFileConfig)

////////////////////////////////////////////////////////////////////////////////

class TLogWriterLivenessCheckerConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enable;

    TDuration LivenessCheckPeriod;

    REGISTER_YSON_STRUCT(TLogWriterLivenessCheckerConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("enable", &TThis::Enable)
            .Default(false);

        registrar.Parameter("liveness_check_period", &TThis::LivenessCheckPeriod)
            .Default();

        registrar.Postprocessor([] (TThis* config) {
            if (config->Enable && !config->LivenessCheckPeriod) {
                THROW_ERROR_EXCEPTION("Log writer liveness checker is enabled while liveness_check_period is not set");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TLogWriterLivenessCheckerConfig)

////////////////////////////////////////////////////////////////////////////////

class TLogTailerConfig
    : public NYTree::TYsonStruct
{
public:
    TLogRotationConfigPtr LogRotation;

    std::vector<TLogFileConfigPtr> LogFiles;

    TLogWriterLivenessCheckerConfigPtr LogWriterLivenessChecker;

    TDuration ReadPeriod;

    //! Read buffer size in bytes.
    i64 ReadBufferSize;
    i64 MaxRecordsPerTransaction;
    i64 MaxRecordsInBuffer;

    TDuration TickPeriod;

    REGISTER_YSON_STRUCT(TLogTailerConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("log_rotation", &TThis::LogRotation)
            .DefaultNew();

        registrar.Parameter("log_files", &TThis::LogFiles)
            .Default();

        registrar.Parameter("log_writer_liveness_checker", &TThis::LogWriterLivenessChecker)
            .Default();

        registrar.Parameter("read_period", &TThis::ReadPeriod)
            .Default(TDuration::Seconds(1));

        registrar.Parameter("read_buffer_size", &TThis::ReadBufferSize)
            .Default(16_MB);

        registrar.Parameter("max_records_per_transaction", &TThis::MaxRecordsPerTransaction)
            .Default(10 * 1000);

        registrar.Parameter("max_records_in_buffer", &TThis::MaxRecordsInBuffer)
            .Default(100 * 1000);

        registrar.Parameter("tick_period", &TThis::TickPeriod)
            .Default(TDuration::MilliSeconds(500));
    }
};

DEFINE_REFCOUNTED_TYPE(TLogTailerConfig)

////////////////////////////////////////////////////////////////////////////////

class TLogTailerBootstrapConfig
    : public TNativeServerConfig
{
public:
    TLogTailerConfigPtr LogTailer;

    TString ClusterUser;

    REGISTER_YSON_STRUCT(TLogTailerBootstrapConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("log_tailer", &TThis::LogTailer);

        registrar.Parameter("cluster_user", &TThis::ClusterUser)
            .Default("yt-log-tailer");
    }
};

DEFINE_REFCOUNTED_TYPE(TLogTailerBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogTailer
