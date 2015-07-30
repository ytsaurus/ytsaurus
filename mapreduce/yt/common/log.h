#pragma once

#include <util/generic/ptr.h>
#include <util/system/compat.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class ILogger
    : public TThrRefBase
{
public:
    enum ELevel
    {
        FATAL,
        ERROR,
        INFO,
        DEBUG
    };

    virtual void Log(ELevel level, const char* format, va_list args) = 0;
};

using ILoggerPtr = TIntrusivePtr<ILogger>;

void SetLogger(ILoggerPtr logger);
ILoggerPtr GetLogger();

ILoggerPtr CreateStdErrLogger(ILogger::ELevel cutLevel);

////////////////////////////////////////////////////////////////////////////////

inline void LogMessage(ILogger::ELevel level, const char* format, ...)
{
    va_list args;
    va_start(args, format);
    GetLogger()->Log(level, format, args);
    va_end(args);
}

#define LOG_DEBUG(...) { LogMessage(ILogger::DEBUG, __VA_ARGS__); }
#define LOG_INFO(...)  { LogMessage(ILogger::INFO, __VA_ARGS__); }
#define LOG_ERROR(...) { LogMessage(ILogger::ERROR, __VA_ARGS__); }
#define LOG_FATAL(...) { LogMessage(ILogger::FATAL, __VA_ARGS__); exit(1); }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
