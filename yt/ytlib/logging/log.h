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

#ifdef ENABLE_TRACE_LOGGING
#define LOG_TRACE(...)                      LOG_EVENT(Logger, ::NYT::NLog::ELogLevel::Trace, __VA_ARGS__)
#define LOG_TRACE_IF(condition, ...)        if (condition) LOG_TRACE(__VA_ARGS__)
#define LOG_TRACE_UNLESS(condition, ...)    if (!condition) LOG_TRACE(__VA_ARGS__)
#else
#define LOG_TRACE(...)                      (void) 0
#define LOG_TRACE_IF(condition, ...)        (void) 0
#define LOG_TRACE_UNLESS(condition, ...)    (void) 0
#endif

#define LOG_DEBUG(...)                      LOG_EVENT(Logger, ::NYT::NLog::ELogLevel::Debug, __VA_ARGS__)
#define LOG_DEBUG_IF(condition, ...)        if (condition) LOG_DEBUG(__VA_ARGS__)
#define LOG_DEBUG_UNLESS(condition, ...)    if (!condition) LOG_DEBUG(__VA_ARGS__)

#define LOG_INFO(...)                       LOG_EVENT(Logger, ::NYT::NLog::ELogLevel::Info, __VA_ARGS__)
#define LOG_INFO_IF(condition, ...)         if (condition) LOG_INFO(__VA_ARGS__)
#define LOG_INFO_UNLESS(condition, ...)     if (!condition) LOG_INFO(__VA_ARGS__)

#define LOG_WARNING(...)                    LOG_EVENT(Logger, ::NYT::NLog::ELogLevel::Warning, __VA_ARGS__)
#define LOG_WARNING_IF(condition, ...)      if (condition) LOG_WARNING(__VA_ARGS__)
#define LOG_WARNING_UNLESS(condition, ...)  if (!condition) LOG_WARNING(__VA_ARGS__)
#define LOG_WARNING_AND_THROW(error)        LOG_EVENT_AND_THROW(Logger, ::NYT::NLog::ELogLevel::Warning, error)

#define LOG_ERROR(...)                      LOG_EVENT(Logger, ::NYT::NLog::ELogLevel::Error, __VA_ARGS__)
#define LOG_ERROR_IF(condition, ...)        if (condition) LOG_ERROR(__VA_ARGS__)
#define LOG_ERROR_UNLESS(condition, ...)    if (!condition) LOG_ERROR(__VA_ARGS__)
#define LOG_ERROR_AND_THROW(error)          LOG_EVENT_AND_THROW(Logger, ::NYT::NLog::ELogLevel::Error, error)

#define LOG_FATAL(...)                      LOG_EVENT(Logger, ::NYT::NLog::ELogLevel::Fatal, __VA_ARGS__)
#define LOG_FATAL_IF(condition, ...)        if ( UNLIKELY(condition)) LOG_FATAL(__VA_ARGS__)
#define LOG_FATAL_UNLESS(condition, ...)    if ( ! LIKELY(condition) ) LOG_FATAL(__VA_ARGS__)

#define LOG_EVENT(logger, level, ...) \
    do { \
        if (logger.IsEnabled(level)) { \
            ::NYT::NLog::LogEventImpl( \
            logger, \
            __FILE__, \
            __LINE__, \
            __FUNCTION__, \
            level, \
            Sprintf(__VA_ARGS__)); \
        } \
    } while (false)

#define LOG_EVENT_AND_THROW(logger, level, error) \
    do { \
        if (logger.IsEnabled(level)) { \
            ::NYT::NLog::LogEventImpl( \
                logger, \
                __FILE__, \
                __LINE__, \
                __FUNCTION__, \
                level, \
                ToString(error)); \
        } \
        THROW_ERROR error; \
    } while (false)


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
