#pragma once

#include "common.h"
#include "writer.h"

#include "../actions/action_queue.h"
#include "../actions/action_util.h"
#include "../actions/async_result.h"

#include <dict/json/json.h>

#include <util/generic/pair.h>
#include <util/datetime/base.h>
#include <util/system/thread.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

class TLogManager
{
public:
    TLogManager();

    static TLogManager* Get();

    void Configure(TJsonObject* root);
    void Configure(Stroka fileName, Stroka rootPath);

    void Flush();
    void Shutdown();

    int GetConfigVersion();
    void GetLoggerConfig(
        Stroka category,
        ELogLevel* minLevel,
        int* configVersion);

    void Write(const TLogEvent& event);

private:
    typedef yvector<ILogWriter::TPtr> TLogWriters;
    typedef ymap<TPair<Stroka, ELogLevel>, TLogWriters> TCachedWriters;

    TLogWriters GetWriters(const TLogEvent& event);
    TLogWriters GetConfiguredWriters(const TLogEvent& event);

    ELogLevel GetMinLevel(Stroka category);

    void DoWrite(const TLogEvent& event);
    TVoid DoFlush();

    // Configuration.
    TSpinLock SpinLock;
    TAtomic ConfigVersion;

    class TConfig;
    TIntrusivePtr<TConfig> Configuration;

    TActionQueue::TPtr Queue;

    TLogWriters SystemWriters;
    TCachedWriters CachedWriters;

    void ConfigureDefault();
    void ConfigureSystem();
};

////////////////////////////////////////////////////////////////////////////////

class TLogger
    : private TNonCopyable
{
public:
    TLogger(Stroka category);

    Stroka GetCategory() const;
    bool IsEnabled(ELogLevel level);
    void Write(const TLogEvent& event);
    
private:
    void UpdateConfig();

    Stroka Category;
    int ConfigVersion;
    ELogLevel MinLevel;
};

////////////////////////////////////////////////////////////////////////////////

class TPrefixLogger
    : private TNonCopyable
{
public:
    TPrefixLogger(TLogger& baseLogger, const Stroka& prefix);

    Stroka GetCategory() const;
    bool IsEnabled(ELogLevel level);
    void Write(const TLogEvent& event);
    
private:
    TLogger& BaseLogger;
    Stroka Prefix;
};

////////////////////////////////////////////////////////////////////////////////

#define LOG_DEBUG(...)                  LOG_EVENT(Logger, ::NYT::NLog::ELogLevel::Debug, __VA_ARGS__)
#define LOG_DEBUG_IF(condition, ...)    if (condition) LOG_DEBUG(__VA_ARGS__)

#define LOG_INFO(...)                   LOG_EVENT(Logger, ::NYT::NLog::ELogLevel::Info, __VA_ARGS__)
#define LOG_INFO_IF(condition, ...)     if (condition) LOG_INFO(__VA_ARGS__)

#define LOG_WARNING(...)                LOG_EVENT(Logger, ::NYT::NLog::ELogLevel::Warning, __VA_ARGS__)
#define LOG_WARNING_IF(condition, ...)  if (condition) LOG_WARNING(__VA_ARGS__)

#define LOG_ERROR(...)                  LOG_EVENT(Logger, ::NYT::NLog::ELogLevel::Error, __VA_ARGS__)
#define LOG_ERROR_IF(condition, ...)    if (condition) LOG_ERROR(__VA_ARGS__)

#define LOG_FATAL(...)                  LOG_EVENT(Logger, ::NYT::NLog::ELogLevel::Fatal, __VA_ARGS__)
#define LOG_FATAL_IF(condition, ...)    if (condition) LOG_FATAL(__VA_ARGS__)

#define LOG_EVENT(logger, level, ...) \
    if (logger.IsEnabled(level)) { \
        Stroka message = Sprintf(__VA_ARGS__); \
        ::NYT::NLog::LogEventImpl( \
            logger, \
            __FILE__, \
            __LINE__, \
            __FUNCTION__, \
            level, \
            message); \
    } \

////////////////////////////////////////////////////////////////////////////////

template<class TLogger>
void LogEventImpl(
    TLogger& logger,
    const char* fileName,
    int line,
    const char* function,
    ELogLevel level,
    const Stroka& message)
{
    TLogEvent event(logger.GetCategory(), level, message);
    event.AddProperty("file", fileName);
    event.AddProperty("line", ToString(line));
    event.AddProperty("thread", ToString(TThread::CurrentThreadId()));
    event.AddProperty("function", function);
    logger.Write(event);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
