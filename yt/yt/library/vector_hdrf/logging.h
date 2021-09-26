#pragma once

#include "public.h"

#include <util/generic/size_literals.h>
#include <util/generic/string.h>

namespace NYT::NVectorHdrf {

////////////////////////////////////////////////////////////////////////////////

// Provides a never-dying singleton with explicit construction.
template <class T>
class TExplicitlyConstructableSingleton
{
public:
    TExplicitlyConstructableSingleton()
    { }

    ~TExplicitlyConstructableSingleton()
    { }

    template <class... Ts>
    void Construct(Ts&&... args)
    {
        new (&Storage_) T(std::forward<Ts>(args)...);
    }

    Y_FORCE_INLINE T* Get()
    {
        return &Storage_;
    }

    Y_FORCE_INLINE const T* Get() const
    {
        return &Storage_;
    }

    Y_FORCE_INLINE T* operator->()
    {
        return Get();
    }

    Y_FORCE_INLINE const T* operator->() const
    {
        return Get();
    }

    Y_FORCE_INLINE T& operator*()
    {
        return *Get();
    }

    Y_FORCE_INLINE const T& operator*() const
    {
        return *Get();
    }

private:
    union {
        T Storage_;
    };
};


////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ELogEventSeverity,
    (Debug)
    (Info)
    (Warning)
    (Error)
);

struct TLogEvent
{
    ELogEventSeverity Severity;
    TStringBuf Message;
};

using TLogHandler = void(*)(const TLogEvent& event);

// Sets the handler to be invoked for each log event produced by VectorHdrf.
// Can be called multiple times (but calls to the previous incarnations of the handler
// are racy).
void EnableLogging(TLogHandler logHandler);

////////////////////////////////////////////////////////////////////////////////

class TLogManager
{
public:
    // Sets the handler to be invoked for each log event produced by VectorHdrf.
    void EnableLogging(TLogHandler logHandler);

    // Checks (in a racy way) that logging is enabled.
    bool IsLoggingEnabled();

    // Logs the message via log handler (if any).
    template <class... Ts>
    void LogMessage(ELogEventSeverity severity, const char* format, Ts&&... args)
    {
        auto logHandler = LogHandler_.load();
        if (!logHandler) {
            return;
        }

        std::array<char, 16_KB> buffer;
        auto len = ::snprintf(buffer.data(), buffer.size(), format, std::forward<Ts>(args)...);

        TLogEvent event;
        event.Severity = severity;
        event.Message = TStringBuf(buffer.data(), len);
        logHandler(event);
    }

    // A special case of zero args.
    void LogMessage(ELogEventSeverity severity, const char* message);

private:
    std::atomic<TLogHandler> LogHandler_ = nullptr;
};

extern TExplicitlyConstructableSingleton<TLogManager> LogManager;

#define YT_VECTOR_HDRF_LOG_EVENT(...)   LogManager->LogMessage(__VA_ARGS__)
#define YT_VECTOR_HDRF_LOG_DEBUG(...)   YT_VECTOR_HDRF_LOG_EVENT(ELogEventSeverity::Debug, __VA_ARGS__)
#define YT_VECTOR_HDRF_LOG_INFO(...)    YT_VECTOR_HDRF_LOG_EVENT(ELogEventSeverity::Info, __VA_ARGS__)
#define YT_VECTOR_HDRF_LOG_WARNING(...) YT_VECTOR_HDRF_LOG_EVENT(ELogEventSeverity::Warning, __VA_ARGS__)
#define YT_VECTOR_HDRF_LOG_ERROR(...)   YT_VECTOR_HDRF_LOG_EVENT(ELogEventSeverity::Error, __VA_ARGS__)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf
