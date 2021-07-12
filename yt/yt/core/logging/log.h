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
    //! This value is used for early dropping of plaintext events in order
    //! to reduce load on logging thread for events which are definitely going
    //! to be dropped due to rule setup.
    //! NB: this optimization is used only for plaintext events since structured
    //! logging rate is negligible comparing to the plaintext logging rate.
    std::atomic<ELogLevel> MinPlainTextLevel;
    std::atomic<int> CurrentVersion;
    std::atomic<int>* ActualVersion;
};

////////////////////////////////////////////////////////////////////////////////

struct TLoggingAnchor
{
    std::atomic<bool> Registered = false;
    ::TSourceLocation SourceLocation = {TStringBuf{}, 0};
    TString AnchorMessage;
    TLoggingAnchor* NextAnchor = nullptr;

    std::atomic<int> CurrentVersion = 0;
    std::atomic<bool> Enabled = false;

    struct TCounter
    {
        std::atomic<i64> Current = 0;
        i64 Previous = 0;
    };

    TCounter MessageCounter;
    TCounter ByteCounter;
};

////////////////////////////////////////////////////////////////////////////////

struct TLogEvent
{
    const TLoggingCategory* Category = nullptr;
    ELogLevel Level = ELogLevel::Minimum;
    ELogFamily Family = ELogFamily::PlainText;
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

    bool IsAnchorUpToDate(const TLoggingAnchor& anchor) const;
    void UpdateAnchor(TLoggingAnchor* anchor) const;
    void RegisterStaticAnchor(TLoggingAnchor* anchor, ::TSourceLocation sourceLocation, TStringBuf message) const;

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

//! Typically serves as a virtual base for classes that need a member logger.
class TLoggerOwner
{
protected:
    TLogger Logger;

    TLoggerOwner() = default;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);
    void Persist(const TStreamPersistenceContext& context);
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
    YT_LOG_EVENT_WITH_ANCHOR(logger, level, nullptr, __VA_ARGS__)

#define YT_LOG_EVENT_WITH_ANCHOR(logger, level, anchor, ...) \
    do { \
        const auto& logger__##__LINE__ = (logger); \
        auto level__##__LINE__ = (level); \
        \
        if (!logger__##__LINE__.IsLevelEnabled(level__##__LINE__)) { \
            break; \
        } \
        \
        auto location__##__LINE__ = __LOCATION__; \
        \
        ::NYT::NLogging::TLoggingAnchor* anchor__##__LINE__ = (anchor); \
        if (!anchor__##__LINE__) { \
            static ::NYT::NLogging::TLoggingAnchor staticAnchor__##__LINE__; \
            anchor__##__LINE__ = &staticAnchor__##__LINE__; \
        } \
        \
        bool anchorUpToDate__##__LINE__ = logger__##__LINE__.IsAnchorUpToDate(*anchor__##__LINE__); \
        if (anchorUpToDate__##__LINE__ && !anchor__##__LINE__->Enabled.load(std::memory_order_relaxed)) { \
            break; \
        } \
        \
        const auto* traceContext__##__LINE__ = ::NYT::NTracing::GetCurrentTraceContext(); \
        auto message__##__LINE__ = ::NYT::NLogging::NDetail::BuildLogMessage(traceContext__##__LINE__, logger__##__LINE__, __VA_ARGS__); \
        \
        if (!anchorUpToDate__##__LINE__) { \
            logger__##__LINE__.RegisterStaticAnchor(anchor__##__LINE__, location__##__LINE__, message__##__LINE__.Anchor); \
            logger__##__LINE__.UpdateAnchor(anchor__##__LINE__); \
        } \
        \
        if (!anchor__##__LINE__->Enabled.load(std::memory_order_relaxed)) { \
            break; \
        } \
        \
        static thread_local i64 localByteCounter__##__LINE__; \
        static thread_local ui8 localMessageCounter__##__LINE__; \
        \
        localByteCounter__##__LINE__ += message__##__LINE__.Message.Size(); \
        if (Y_UNLIKELY(++localMessageCounter__##__LINE__ == 0)) { \
            anchor__##__LINE__->MessageCounter.Current += 256; \
            anchor__##__LINE__->ByteCounter.Current += localByteCounter__##__LINE__; \
            localByteCounter__##__LINE__ = 0; \
        } \
        \
        ::NYT::NLogging::NDetail::LogEventImpl( \
            traceContext__##__LINE__, \
            logger__##__LINE__, \
            level__##__LINE__, \
            location__##__LINE__, \
            std::move(message__##__LINE__.Message)); \
    } while (false)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging

#define LOG_INL_H_
#include "log-inl.h"
#undef LOG_INL_H_
