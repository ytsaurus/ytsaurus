#pragma once

#include "common.h"

#include <util/system/thread.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

class TLogManager;

class TLogger
{
public:
    explicit TLogger(const Stroka& category = "");
    TLogger(const TLogger& other);

    Stroka GetCategory() const;
    bool IsEnabled(ELogLevel level) const;
    void Write(const TLogEvent& event);
    
private:
    TLogManager* GetLogManager() const;
    void UpdateConfig();

    Stroka Category;
    int ConfigVersion;
    mutable TLogManager* LogManager;
    ELogLevel MinLevel;
};

////////////////////////////////////////////////////////////////////////////////

#define LOG_TRACE(...)                  LOG_EVENT(Logger, ::NYT::NLog::ELogLevel::Trace, __VA_ARGS__)
#define LOG_TRACE_IF(condition, ...)    if (condition) LOG_TRACE(__VA_ARGS__)

#define LOG_DEBUG(...)                  LOG_EVENT(Logger, ::NYT::NLog::ELogLevel::Debug, __VA_ARGS__)
#define LOG_DEBUG_IF(condition, ...)    if (condition) LOG_DEBUG(__VA_ARGS__)

#define LOG_INFO(...)                   LOG_EVENT(Logger, ::NYT::NLog::ELogLevel::Info, __VA_ARGS__)
#define LOG_INFO_IF(condition, ...)     if (condition) LOG_INFO(__VA_ARGS__)

#define LOG_WARNING(...)                LOG_EVENT(Logger, ::NYT::NLog::ELogLevel::Warning, __VA_ARGS__)
#define LOG_WARNING_IF(condition, ...)  if (condition) LOG_WARNING(__VA_ARGS__)
#define LOG_WARNING_AND_THROW(ex, ...)  LOG_EVENT_AND_THROW(Logger, ::NYT::NLog::ELogLevel::Warning, ex, __VA_ARGS__)

#define LOG_ERROR(...)                   LOG_EVENT(Logger, ::NYT::NLog::ELogLevel::Error, __VA_ARGS__)
#define LOG_ERROR_IF(condition, ...)     if (condition) LOG_ERROR(__VA_ARGS__)
#define LOG_ERROR_AND_THROW(ex, ...)     LOG_EVENT_AND_THROW(Logger, ::NYT::NLog::ELogLevel::Error, ex, __VA_ARGS__)

#define LOG_FATAL(...)                  LOG_EVENT(Logger, ::NYT::NLog::ELogLevel::Fatal, __VA_ARGS__)
#define LOG_FATAL_IF(condition, ...)    if (condition) LOG_FATAL(__VA_ARGS__)

#define LOG_EVENT(logger, level, ...) \
    if (logger.IsEnabled(level)) { \
        ::NYT::NLog::LogEventImpl( \
            logger, \
            __FILE__, \
            __LINE__, \
            __FUNCTION__, \
            level, \
            Sprintf(__VA_ARGS__)); \
    } \

#define LOG_EVENT_AND_THROW(logger, level, ex, ...) \
    if (logger.IsEnabled(level)) { \
        Stroka message = Sprintf(__VA_ARGS__); \
        ::NYT::NLog::LogEventImpl( \
            logger, \
            __FILE__, \
            __LINE__, \
            __FUNCTION__, \
            level, \
            message); \
        ythrow ex << message; \
    } \

////////////////////////////////////////////////////////////////////////////////

template <class TLogger>
void LogEventImpl(
    TLogger& logger,
    const char* fileName,
    int line,
    const char* function,
    ELogLevel level,
    const Stroka& message)
{
    TLogEvent event;
    event.Category = logger.GetCategory();
    event.Level = level;
    event.Message = message;
    event.FileName = fileName;
    event.Line = line;
    event.ThreadId = NThread::GetCurrentThreadId();
    event.Function = function;
    logger.Write(event);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
