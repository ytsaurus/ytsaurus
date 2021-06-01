#include "formatter.h"
#include "private.h"
#include "log.h"

#include <yt/yt/build/build.h>

#include <yt/yt/core/json/json_writer.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/yson/writer.h>

#include <util/stream/length.h>

namespace NYT::NLogging {

using namespace NProfiling;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const TLogger Logger(SystemLoggingCategoryName);

namespace {

TLogEvent GetStartLogEvent()
{
    TLogEvent event;
    event.Instant = GetCpuInstant();
    event.Category = Logger.GetCategory();
    event.Level = ELogLevel::Info;
    event.Message = TSharedRef::FromString(Format("Logging started (Version: %v, BuildHost: %v, BuildTime: %v)",
        GetVersion(),
        GetBuildHost(),
        GetBuildTime()));
    return event;
}

TLogEvent GetStartLogStructuredEvent()
{
    TLogEvent event;
    event.Instant = GetCpuInstant();
    event.Category = Logger.GetCategory();
    event.Level = ELogLevel::Info;
    event.StructuredMessage = BuildYsonStringFluently<NYson::EYsonType::MapFragment>()
        .Item("message").Value("Logging started")
        .Item("version").Value(GetVersion())
        .Item("build_host").Value(GetBuildHost())
        .Item("build_time").Value(GetBuildTime())
        .Finish();
    return event;
}

TLogEvent GetSkippedLogEvent(i64 count, TStringBuf skippedBy)
{
    TLogEvent event;
    event.Instant = GetCpuInstant();
    event.Category = Logger.GetCategory();
    event.Level = ELogLevel::Info;
    event.Message = TSharedRef::FromString(Format("Skipped log records in last second (Count: %v, SkippedBy: %v)",
        count,
        skippedBy));
    return event;
}

TLogEvent GetSkippedLogStructuredEvent(i64 count, TStringBuf skippedBy)
{
    TLogEvent event;
    event.Instant = GetCpuInstant();
    event.Category = Logger.GetCategory();
    event.Level = ELogLevel::Info;
    event.StructuredMessage = BuildYsonStringFluently<NYson::EYsonType::MapFragment>()
        .Item("message").Value("Events skipped")
        .Item("skipped_by").Value(skippedBy)
        .Item("events_skipped").Value(count)
        .Finish();
    return event;
}

} // namespace


TCachingDateFormatter::TCachingDateFormatter()
{
    Update(GetCpuInstant());
}

const char* TCachingDateFormatter::Format(TCpuInstant instant)
{
    if (instant <= Liveline_ || instant >= Deadline_) {
        Update(instant);
    }
    return Cached_.GetData();
}

void TCachingDateFormatter::Update(TCpuInstant instant)
{
    Cached_.Reset();
    FormatDateTime(&Cached_, CpuInstantToInstant(instant));
    Cached_.AppendChar('\0');
    auto period = DurationToCpuDuration(TDuration::MicroSeconds(500));
    Deadline_ = instant + period;
    Liveline_ = instant - period;
}

////////////////////////////////////////////////////////////////////////////////

TPlainTextLogFormatter::TPlainTextLogFormatter(
    bool enableControlMessages,
    bool enableSourceLocation)
    : Buffer_(std::make_unique<TMessageBuffer>())
    , CachingDateFormatter_(std::make_unique<TCachingDateFormatter>())
    , EnableSystemMessages_(enableControlMessages)
    , EnableSourceLocation_(enableSourceLocation)
{ }

i64 TPlainTextLogFormatter::WriteFormatted(IOutputStream* outputStream, const TLogEvent& event) const
{
    if (!outputStream) {
        return 0;
    }

    auto* buffer = Buffer_.get();
    buffer->Reset();

    buffer->AppendString(CachingDateFormatter_->Format(event.Instant));

    buffer->AppendChar('\t');

    FormatLevel(buffer, event.Level);

    buffer->AppendChar('\t');

    buffer->AppendString(event.Category->Name);

    buffer->AppendChar('\t');

    // COMPAT(babenko)
    if (event.Level == ELogLevel::Alert) {
        buffer->AppendString(TStringBuf("Unexpected error: "));
    }
    FormatMessage(buffer, TStringBuf(event.Message.Begin(), event.Message.End()));

    buffer->AppendChar('\t');

    if (event.ThreadNameLength > 0) {
        buffer->AppendString(TStringBuf(event.ThreadName.data(), event.ThreadNameLength));
    } else if (event.ThreadId != NConcurrency::InvalidThreadId) {
        buffer->AppendNumber(event.ThreadId, 16);
    }

    buffer->AppendChar('\t');

    if (event.FiberId != NConcurrency::InvalidFiberId) {
        buffer->AppendNumber(event.FiberId, 16);
    }

    buffer->AppendChar('\t');

    if (event.TraceId != NTracing::InvalidTraceId) {
        buffer->AppendGuid(event.TraceId);
    }

    if (EnableSourceLocation_) {
        buffer->AppendChar('\t');
        if (event.SourceFile) {
            auto sourceFile = event.SourceFile;
            buffer->AppendString(sourceFile.RNextTok(LOCSLASH_C));
            buffer->AppendChar(':');
            buffer->AppendNumber(event.SourceLine);
        }
    }

    buffer->AppendChar('\n');

    outputStream->Write(buffer->GetData(), buffer->GetBytesWritten());

    return buffer->GetBytesWritten();
}

void TPlainTextLogFormatter::WriteLogReopenSeparator(IOutputStream* outputStream) const
{
    *outputStream << Endl;
}

void TPlainTextLogFormatter::WriteLogStartEvent(IOutputStream* outputStream) const
{
    if (EnableSystemMessages_) {
        WriteFormatted(outputStream, GetStartLogEvent());
    }
}

void TPlainTextLogFormatter::WriteLogSkippedEvent(IOutputStream* outputStream, i64 count, TStringBuf skippedBy) const
{
    if (EnableSystemMessages_) {
        WriteFormatted(outputStream, GetSkippedLogEvent(count, skippedBy));
    }
}

////////////////////////////////////////////////////////////////////////////////

TStructuredLogFormatter::TStructuredLogFormatter(ELogFormat format, const THashMap<TString, NYTree::INodePtr>& commonFields, bool enableControlMessages)
    : Format_(format)
    , CachingDateFormatter_(std::make_unique<TCachingDateFormatter>())
    , CommonFields_(commonFields)
    , EnableSystemMessages_(enableControlMessages)
{ }

i64 TStructuredLogFormatter::WriteFormatted(IOutputStream* stream, const TLogEvent& event) const
{
    if (!stream) {
        return 0;
    }

    auto countingStream = TCountingOutput(stream);
    std::unique_ptr<IFlushableYsonConsumer> consumer;

    switch (Format_) {
        case ELogFormat::Json:
            consumer = NJson::CreateJsonConsumer(&countingStream);
            break;
        case ELogFormat::Yson:
            consumer = std::make_unique<TYsonWriter>(&countingStream, EYsonFormat::Text);
            break;
        default:
            YT_ABORT();
    }

    BuildYsonFluently(consumer.get())
        .BeginMap()
            .DoFor(CommonFields_, [] (auto fluent, auto item) {
                fluent.Item(item.first).Value(item.second);
            })
            .Items(event.StructuredMessage)
            .Item("instant").Value(ToString(CachingDateFormatter_->Format(event.Instant)))
            .Item("level").Value(FormatEnum(event.Level))
            .Item("category").Value(event.Category->Name)
        .EndMap();
    consumer->Flush();

    if (Format_ == ELogFormat::Yson) {
        // In order to obtain proper list fragment, we must manually insert trailing semicolon in each line.
        countingStream.Write(';');
    }

    countingStream.Write('\n');

    return countingStream.Counter();
}

void TStructuredLogFormatter::WriteLogReopenSeparator(IOutputStream* /*outputStream*/) const
{ }

void TStructuredLogFormatter::WriteLogStartEvent(IOutputStream* outputStream) const
{
    if (EnableSystemMessages_) {
        WriteFormatted(outputStream, GetStartLogStructuredEvent());
    }
}

void TStructuredLogFormatter::WriteLogSkippedEvent(IOutputStream* outputStream, i64 count, TStringBuf skippedBy) const
{
    if (EnableSystemMessages_) {
        WriteFormatted(outputStream, GetSkippedLogStructuredEvent(count, skippedBy));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
