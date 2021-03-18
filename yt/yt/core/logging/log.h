#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/format.h>
#include <yt/yt/core/misc/ref.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/profiling/public.h>

#include <util/system/thread.h>
#include <util/system/src_location.h>

#include <atomic>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

struct TLoggingCategory
{
    TStringBuf Name;
    std::atomic<ELogLevel> MinLevel;
    std::atomic<int> CurrentVersion;
    std::atomic<int>* ActualVersion;
};

////////////////////////////////////////////////////////////////////////////////

struct TLoggingPosition
{
    std::atomic<bool> Enabled = false;
    std::atomic<int> CurrentVersion = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TLogEvent
{
    const TLoggingCategory* Category = nullptr;
    ELogLevel Level = ELogLevel::Minimum;
    ELogMessageFormat MessageFormat = ELogMessageFormat::PlainText;
    bool Essential = false;

    TSharedRef Message;
    NYson::TYsonString StructuredMessage;

    NProfiling::TCpuInstant Instant = 0;

    NConcurrency::TThreadId ThreadId = NConcurrency::InvalidThreadId;

    static constexpr int ThreadNameBufferSize = 16 ; // including zero terminator
    using TThreadName = std::array<char, ThreadNameBufferSize>;
    TThreadName ThreadName; // zero-terminated
    int ThreadNameLength = 0; // not including zero terminator

    NConcurrency::TFiberId FiberId = NConcurrency::InvalidFiberId;

    NTracing::TTraceId TraceId = NTracing::InvalidTraceId;

    NTracing::TRequestId RequestId = NTracing::InvalidRequestId;
    TStringBuf SourceFile;
    int SourceLine = -1;
};

////////////////////////////////////////////////////////////////////////////////

static constexpr auto NullLoggerMinLevel = ELogLevel::Maximum;

// Min level for non-null logger depends on whether we are in debug or release build.
// - For release mode default behavior is to omit trace logging,
//   this is done by setting logger min level to Debug by default.
// - For debug mode logger min level is set to trace by default, so that trace logging is
//   allowed by logger, but still may be discarded by category min level.
#ifdef NDEBUG
static constexpr auto LoggerDefaultMinLevel = ELogLevel::Debug;
#else
static constexpr auto LoggerDefaultMinLevel = ELogLevel::Trace;
#endif

class TLogger
{
public:
    TLogger() = default;
    TLogger(const TLogger& other) = default;
    explicit TLogger(TStringBuf categoryName);

    explicit operator bool() const;

    const TLoggingCategory* GetCategory() const;

    //! Validate that level is admitted by logger's own min level
    //! and by category's min level.
    bool IsLevelEnabled(ELogLevel level) const;

    bool GetAbortOnAlert() const;

    bool IsEssential() const;

    bool IsPositionUpToDate(const TLoggingPosition& position) const;
    void UpdatePosition(TLoggingPosition* position, TStringBuf message) const;

    void Write(TLogEvent&& event) const;

    void AddRawTag(const TString& tag);
    template <class... TArgs>
    void AddTag(const char* format, TArgs&&... args);

    TLogger WithRawTag(const TString& tag) const;
    template <class... TArgs>
    TLogger WithTag(const char* format, TArgs&&... args) const;

    TLogger WithMinLevel(ELogLevel minLevel) const;

    TLogger WithEssential(bool essential = true) const;

    const TString& GetTag() const;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

private:
    // These fields are set only during logger creation, so they are effectively const
    // and accessing them is thread-safe.
    TLogManager* LogManager_ = nullptr;
    const TLoggingCategory* Category_ = nullptr;
    bool Essential_ = false;
    ELogLevel MinLevel_ = NullLoggerMinLevel;
    TString Tag_;

    //! This method checks level against category's min level.
    //! Refer to comment in TLogger::IsLevelEnabled for more details.
    bool IsLevelEnabledHeavy(ELogLevel level) const;
};

////////////////////////////////////////////////////////////////////////////////

extern TLogger NullLogger;

////////////////////////////////////////////////////////////////////////////////

//! Typically serves as a virtual base for classes that need a member logger.
class TLoggerOwner
{
protected:
    TLogger Logger;

};

////////////////////////////////////////////////////////////////////////////////

void LogStructuredEvent(const TLogger& logger,
    NYson::TYsonString message,
    ELogLevel level);

////////////////////////////////////////////////////////////////////////////////

#ifdef YT_ENABLE_TRACE_LOGGING
#define YT_LOG_TRACE(...)                      YT_LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Trace, __VA_ARGS__)
#define YT_LOG_TRACE_IF(condition, ...)        if (condition)    YT_LOG_TRACE(__VA_ARGS__)
#define YT_LOG_TRACE_UNLESS(condition, ...)    if (!(condition)) YT_LOG_TRACE(__VA_ARGS__)
#else
#define YT_LOG_UNUSED(...)                     if (true) { } else { YT_LOG_DEBUG(__VA_ARGS__); }
#define YT_LOG_TRACE(...)                      YT_LOG_UNUSED(__VA_ARGS__)
#define YT_LOG_TRACE_IF(condition, ...)        YT_LOG_UNUSED(__VA_ARGS__)
#define YT_LOG_TRACE_UNLESS(condition, ...)    YT_LOG_UNUSED(__VA_ARGS__)
#endif

#define YT_LOG_DEBUG(...)                      YT_LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Debug, __VA_ARGS__)
#define YT_LOG_DEBUG_IF(condition, ...)        if (condition)    YT_LOG_DEBUG(__VA_ARGS__)
#define YT_LOG_DEBUG_UNLESS(condition, ...)    if (!(condition)) YT_LOG_DEBUG(__VA_ARGS__)

#define YT_LOG_INFO(...)                       YT_LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Info, __VA_ARGS__)
#define YT_LOG_INFO_IF(condition, ...)         if (condition)    YT_LOG_INFO(__VA_ARGS__)
#define YT_LOG_INFO_UNLESS(condition, ...)     if (!(condition)) YT_LOG_INFO(__VA_ARGS__)

#define YT_LOG_WARNING(...)                    YT_LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Warning, __VA_ARGS__)
#define YT_LOG_WARNING_IF(condition, ...)      if (condition)    YT_LOG_WARNING(__VA_ARGS__)
#define YT_LOG_WARNING_UNLESS(condition, ...)  if (!(condition)) YT_LOG_WARNING(__VA_ARGS__)

#define YT_LOG_ERROR(...)                      YT_LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Error, __VA_ARGS__)
#define YT_LOG_ERROR_IF(condition, ...)        if (condition)    YT_LOG_ERROR(__VA_ARGS__)
#define YT_LOG_ERROR_UNLESS(condition, ...)    if (!(condition)) YT_LOG_ERROR(__VA_ARGS__)

#define YT_LOG_ALERT(...) \
    do { \
        YT_LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Alert, __VA_ARGS__); \
        YT_VERIFY(!Logger.GetAbortOnAlert()); \
    } while(false)
#define YT_LOG_ALERT_IF(condition, ...)        if (condition)    YT_LOG_ALERT(__VA_ARGS__)
#define YT_LOG_ALERT_UNLESS(condition, ...)    if (!(condition)) YT_LOG_ALERT(__VA_ARGS__)

#define YT_LOG_FATAL(...) \
    do { \
        YT_LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Fatal, __VA_ARGS__); \
        YT_ABORT(); \
    } while(false)
#define YT_LOG_FATAL_IF(condition, ...)        if (Y_UNLIKELY(condition)) YT_LOG_FATAL(__VA_ARGS__)
#define YT_LOG_FATAL_UNLESS(condition, ...)    if (!Y_LIKELY(condition)) YT_LOG_FATAL(__VA_ARGS__)

#define YT_LOG_EVENT(logger, level, ...) \
    do { \
        if (!logger.IsLevelEnabled(level)) { \
            break; \
        } \
        \
        static ::NYT::NLogging::TLoggingPosition position__##__LINE__; \
        bool positionUpToDate__##__LINE__ = logger.IsPositionUpToDate(position__##__LINE__); \
        if (positionUpToDate__##__LINE__ && !position__##__LINE__.Enabled) { \
            break; \
        } \
        \
        const auto* traceContext__##__LINE__ = ::NYT::NTracing::GetCurrentTraceContext(); \
        auto message__##__LINE__ = ::NYT::NLogging::NDetail::BuildLogMessage(traceContext__##__LINE__, logger, __VA_ARGS__); \
        if (!positionUpToDate__##__LINE__) { \
            logger.UpdatePosition(&position__##__LINE__, TStringBuf(message__##__LINE__.Begin(), message__##__LINE__.Size())); \
        } \
        \
        if (position__##__LINE__.Enabled) { \
            ::NYT::NLogging::NDetail::LogEventImpl( \
                traceContext__##__LINE__, \
                logger, \
                level, \
                __LOCATION__, \
                std::move(message__##__LINE__)); \
        } \
    } while (false)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging

#define LOG_INL_H_
#include "log-inl.h"
#undef LOG_INL_H_
