#pragma once
#ifndef LOG_INL_H_
#error "Direct inclusion of this file is not allowed, include log.h"
// For the sake of sane code completion.
#include "log.h"
#endif
#undef LOG_INL_H_

#include <yt/core/profiling/timing.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

template <class... TArgs>
TLogger& TLogger::AddTag(const char* format, TArgs&&... args)
{
    return AddRawTag(Format(format, std::forward<TArgs>(args)...));
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

struct TMessageStringBuilderContext
{
    TSharedMutableRef Chunk;
};

struct TMessageBufferTag
{ };

class TMessageStringBuilder
    : public TStringBuilderBase
{
public:
    TSharedRef Flush();

    // For testing only.
    static void DisablePerThreadCache();

protected:
    virtual void DoReset() override;
    virtual void DoPreallocate(size_t newLength) override;

private:
    struct TPerThreadCache
    {
        ~TPerThreadCache();

        TSharedMutableRef Chunk;
        size_t ChunkOffset = 0;
    };

    TSharedMutableRef Buffer_;

#ifndef __APPLE__
    static thread_local TPerThreadCache* Cache_;
    static thread_local bool CacheDestroyed_;
#endif
    static TPerThreadCache* GetCache();

    static constexpr size_t ChunkSize = 64_KB;
};

template <class... TArgs>
void AppendLogMessage(TStringBuilderBase* builder, TStringBuf context, TRef message)
{
    if (context) {
        if (message.Size() >= 1 && message[message.Size() - 1] == ')') {
            builder->AppendString(TStringBuf(message.Begin(), message.Size() - 1));
            builder->AppendString(AsStringBuf(", "));
            builder->AppendString(context);
            builder->AppendChar(')');
        } else {
            builder->AppendString(TStringBuf(message.Begin(), message.Size()));
            builder->AppendString(AsStringBuf(" ("));
            builder->AppendString(context);
            builder->AppendChar(')');
        }
    } else {
        builder->AppendString(TStringBuf(message.Begin(), message.Size()));
    }
}

template <class... TArgs, size_t FormatLength>
void AppendLogMessageWithFormat(TStringBuilderBase* builder, TStringBuf context, const char (&format)[FormatLength], TArgs&&... args)
{
    if (context) {
        if (FormatLength >= 2 && format[FormatLength - 2] == ')') {
            builder->AppendFormat(TStringBuf(format, FormatLength - 2), std::forward<TArgs>(args)...);
            builder->AppendString(AsStringBuf(", "));
            builder->AppendString(context);
            builder->AppendChar(')');
        } else {
            builder->AppendFormat(format, std::forward<TArgs>(args)...);
            builder->AppendString(AsStringBuf(" ("));
            builder->AppendString(context);
            builder->AppendChar(')');
        }
    } else {
        builder->AppendFormat(format, std::forward<TArgs>(args)...);
    }
}

template <class... TArgs, size_t FormatLength>
TSharedRef BuildLogMessage(TStringBuf context, const char (&format)[FormatLength], TArgs&&... args)
{
    TMessageStringBuilder builder;
    AppendLogMessageWithFormat(&builder, context, format, std::forward<TArgs>(args)...);
    return builder.Flush();
}

template <class... TArgs, size_t FormatLength>
TSharedRef BuildLogMessage(TStringBuf context, const TError& error, const char (&format)[FormatLength], TArgs&&... args)
{
    TMessageStringBuilder builder;
    AppendLogMessageWithFormat(&builder, context, format, std::forward<TArgs>(args)...);
    builder.AppendChar('\n');
    FormatValue(&builder, error, TStringBuf());
    return builder.Flush();
}

template <class T>
TSharedRef BuildLogMessage(TStringBuf context, const T& obj)
{
    TMessageStringBuilder builder;
    FormatValue(&builder, obj, TStringBuf());
    if (context) {
        builder.AppendString(AsStringBuf(" ("));
        builder.AppendString(context);
        builder.AppendChar(')');
    }
    return builder.Flush();
}

inline TSharedRef BuildLogMessage(TStringBuf context, TSharedRef&& message)
{
    if (context) {
        TMessageStringBuilder builder;
        AppendLogMessage(&builder, context, message);
        return builder.Flush();
    } else {
        return std::move(message);
    }
}

inline TLogEvent CreateLogEvent(const TLogger& logger, ELogLevel level)
{
    TLogEvent event;
    event.Instant = NProfiling::GetCpuInstant();
    event.Category = logger.GetCategory();
    event.Level = level;
    event.ThreadId = TThread::CurrentThreadId();
    event.FiberId = NConcurrency::GetCurrentFiberId();
    event.TraceId = NTracing::GetCurrentTraceId();
    return event;
}

inline void LogEventImpl(
    const TLogger& logger,
    ELogLevel level,
    TSharedRef message)
{
    auto event = CreateLogEvent(logger, level);
    event.Message = std::move(message);
    event.MessageFormat = ELogMessageFormat::PlainText;
    logger.Write(std::move(event));
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

inline void LogStructuredEvent(
    const TLogger& logger,
    NYson::TYsonString message,
    ELogLevel level)
{
    YT_VERIFY(message.GetType() == NYson::EYsonType::MapFragment);
    TLogEvent event = NDetail::CreateLogEvent(logger, level);
    event.StructuredMessage = std::move(message);
    event.MessageFormat = ELogMessageFormat::Structured;
    logger.Write(std::move(event));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
