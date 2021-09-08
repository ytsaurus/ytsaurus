#pragma once
#ifndef LOG_INL_H_
#error "Direct inclusion of this file is not allowed, include log.h"
// For the sake of sane code completion.
#include "log.h"
#endif
#undef LOG_INL_H_

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

inline bool TLogger::IsAnchorUpToDate(const TLoggingAnchor& position) const
{
    return
        !Category_ ||
        position.CurrentVersion == Category_->ActualVersion->load(std::memory_order_relaxed);
}

template <class... TArgs>
void TLogger::AddTag(const char* format, TArgs&&... args)
{
    AddRawTag(Format(format, std::forward<TArgs>(args)...));
}

template <class TType>
void TLogger::AddStructuredTag(TStringBuf key, TType value)
{
    StructuredTags_.emplace_back(key, NYTree::ConvertToYsonString(value));
}

template <class... TArgs>
TLogger TLogger::WithTag(const char* format, TArgs&&... args) const
{
    auto result = *this;
    result.AddTag(format, std::forward<TArgs>(args)...);
    return result;
}

template <class TType>
TLogger TLogger::WithStructuredTag(TStringBuf key, TType value) const
{
    auto result = *this;
    result.AddStructuredTag(key, value);
    return result;
}

Y_FORCE_INLINE bool TLogger::IsLevelEnabled(ELogLevel level) const
{
    // This is the first check which is intended to be inlined next to
    // logging invocation point. Check below is almost zero-cost due
    // to branch prediction (which requires inlining for proper work).
    if (level < MinLevel_) {
        return false;
    }

    // Next check is heavier and requires full log manager definition which
    // is undesirable in -inl.h header file. This is why we extract it
    // to a separate method which is implemented in cpp file.
    return IsLevelEnabledHeavy(level);
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

    static thread_local TPerThreadCache* Cache_;
    static thread_local bool CacheDestroyed_;
    static TPerThreadCache* GetCache();

    static constexpr size_t ChunkSize = 64_KB;
};

inline bool HasMessageTags(
    const NTracing::TTraceContext* traceContext,
    const TLogger& logger)
{
    if (logger.GetTag()) {
        return true;
    }
    if (traceContext && traceContext->GetLoggingTag()) {
        return true;
    }
    return false;
}

inline void AppendMessageTags(
    TStringBuilderBase* builder,
    const NTracing::TTraceContext* traceContext,
    const TLogger& logger)
{
    bool printComma = false;
    if (const auto& loggerTag = logger.GetTag()) {
        builder->AppendString(loggerTag);
        printComma = true;
    }
    if (traceContext) {
        if (const auto& traceContextTag = traceContext->GetLoggingTag()) {
            if (printComma) {
                builder->AppendString(TStringBuf(", "));
            }
            builder->AppendString(traceContextTag);
        }
    }
}

template <class... TArgs>
void AppendLogMessage(
    TStringBuilderBase* builder,
    const NTracing::TTraceContext* traceContext,
    const TLogger& logger,
    TRef message)
{
    if (HasMessageTags(traceContext, logger)) {
        if (message.Size() >= 1 && message[message.Size() - 1] == ')') {
            builder->AppendString(TStringBuf(message.Begin(), message.Size() - 1));
            builder->AppendString(TStringBuf(", "));
        } else {
            builder->AppendString(TStringBuf(message.Begin(), message.Size()));
            builder->AppendString(TStringBuf(" ("));
        }
        AppendMessageTags(builder, traceContext, logger);
        builder->AppendChar(')');
    } else {
        builder->AppendString(TStringBuf(message.Begin(), message.Size()));
    }
}

template <class... TArgs, size_t FormatLength>
void AppendLogMessageWithFormat(
    TStringBuilderBase* builder,
    const NTracing::TTraceContext* traceContext,
    const TLogger& logger,
    const char (&format)[FormatLength],
    TArgs&&... args)
{
    if (HasMessageTags(traceContext, logger)) {
        if (FormatLength >= 2 && format[FormatLength - 2] == ')') {
            builder->AppendFormat(TStringBuf(format, FormatLength - 2), std::forward<TArgs>(args)...);
            builder->AppendString(TStringBuf(", "));
        } else {
            builder->AppendFormat(format, std::forward<TArgs>(args)...);
            builder->AppendString(TStringBuf(" ("));
        }
        AppendMessageTags(builder, traceContext, logger);
        builder->AppendChar(')');
    } else {
        builder->AppendFormat(format, std::forward<TArgs>(args)...);
    }
}

struct TLogMessage
{
    TSharedRef Message;
    TStringBuf Anchor;
};

template <class... TArgs, size_t FormatLength>
TLogMessage BuildLogMessage(
    const NTracing::TTraceContext* traceContext,
    const TLogger& logger,
    const char (&format)[FormatLength],
    TArgs&&... args)
{
    TMessageStringBuilder builder;
    AppendLogMessageWithFormat(&builder, traceContext, logger, format, std::forward<TArgs>(args)...);
    return {builder.Flush(), AsStringBuf(format)};
}

template <class... TArgs, size_t FormatLength>
TLogMessage BuildLogMessage(
    const NTracing::TTraceContext* traceContext,
    const TLogger& logger,
    const TError& error,
    const char (&format)[FormatLength],
    TArgs&&... args)
{
    TMessageStringBuilder builder;
    AppendLogMessageWithFormat(&builder, traceContext, logger, format, std::forward<TArgs>(args)...);
    builder.AppendChar('\n');
    FormatValue(&builder, error, TStringBuf());
    return {builder.Flush(), AsStringBuf(format)};
}

template <class T>
TLogMessage BuildLogMessage(
    const NTracing::TTraceContext* traceContext,
    const TLogger& logger,
    const T& obj)
{
    TMessageStringBuilder builder;
    FormatValue(&builder, obj, TStringBuf());
    if (HasMessageTags(traceContext, logger)) {
        builder.AppendString(TStringBuf(" ("));
        AppendMessageTags(&builder, traceContext, logger);
        builder.AppendChar(')');
    }
    return {builder.Flush(), TStringBuf()};
}

inline TLogMessage BuildLogMessage(
    const NTracing::TTraceContext* traceContext,
    const TLogger& logger,
    TSharedRef&& message)
{
    if (HasMessageTags(traceContext, logger)) {
        TMessageStringBuilder builder;
        AppendLogMessage(&builder, traceContext, logger, message);
        return {builder.Flush(), TStringBuf()};
    } else {
        return {std::move(message), TStringBuf()};
    }
}

extern thread_local bool CachedThreadNameInitialized;
extern thread_local TLogEvent::TThreadName CachedThreadName;
extern thread_local int CachedThreadNameLength;

void CacheThreadName();

inline TLogEvent CreateLogEvent(
    const NTracing::TTraceContext* traceContext,
    const TLogger& logger,
    ELogLevel level)
{
    if (!CachedThreadNameInitialized) {
        CacheThreadName();
    }
    TLogEvent event;
    event.Instant = NProfiling::GetCpuInstant();
    event.Category = logger.GetCategory();
    event.Essential = logger.IsEssential();
    event.Level = level;
    event.ThreadId = TThread::CurrentThreadId();
    event.ThreadName = CachedThreadName;
    event.ThreadNameLength = CachedThreadNameLength;
    event.FiberId = NConcurrency::GetCurrentFiberId();
    if (traceContext) {
        event.TraceId = traceContext->GetTraceId();
        event.RequestId = traceContext->GetRequestId();
    }
    return event;
}

inline void LogEventImpl(
    const NTracing::TTraceContext* traceContext,
    const TLogger& logger,
    ELogLevel level,
    ::TSourceLocation sourceLocation,
    TSharedRef message)
{
    auto event = CreateLogEvent(traceContext, logger, level);
    event.Message = std::move(message);
    event.Family = ELogFamily::PlainText;
    event.SourceFile = sourceLocation.File;
    event.SourceLine = sourceLocation.Line;
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
    auto event = NDetail::CreateLogEvent(
        NTracing::GetCurrentTraceContext(),
        logger,
        level);
    event.StructuredMessage = std::move(message);
    event.Family = ELogFamily::Structured;
    logger.Write(std::move(event));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
