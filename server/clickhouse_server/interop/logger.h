#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <memory>

namespace NInterop {

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

class ILogger
{
public:
    virtual ~ILogger() = default;

    virtual void Write(const TLogEvent& event) = 0;
};

using ILoggerPtr = std::shared_ptr<ILogger>;

}   // namespace NInterop
