#pragma once

#include "public.h"

#include <yt/core/logging/public.h>

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

enum class ELogLevel
{
    Minimum,
    Trace,
    Debug,
    Info,
    Warning,
    Error,
    Fatal,
    Maximum,
};

////////////////////////////////////////////////////////////////////////////////

struct TLogEvent
{
    ELogLevel Level;
    TString Message;
    TInstant Timestamp;
    ui32 ThreadId;
};

////////////////////////////////////////////////////////////////////////////////

struct ILogger
{
    virtual ~ILogger() = default;

    virtual void Write(const TLogEvent& event) = 0;
};

////////////////////////////////////////////////////////////////////////////////

ILoggerPtr CreateLogger(const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
