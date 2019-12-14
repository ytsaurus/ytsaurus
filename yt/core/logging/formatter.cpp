#include "formatter.h"
#include "private.h"
#include "log.h"
#include "log_manager.h"

#include <yt/build/build.h>

#include <yt/core/json/json_writer.h>
#include <yt/core/ytree/fluent.h>

#include <util/stream/length.h>

namespace NYT::NLogging {

using namespace NProfiling;
using namespace NYTree;

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

TLogEvent GetSkippedLogEvent(i64 count, const TString& skippedBy)
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

TLogEvent GetSkippedLogStructuredEvent(i64 count, const TString& skippedBy)
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

TPlainTextLogFormatter::TPlainTextLogFormatter()
    : Buffer_(new TMessageBuffer())
    , CachingDateFormatter_(new TCachingDateFormatter())
{ }

size_t TPlainTextLogFormatter::WriteFormatted(IOutputStream* outputStream, const TLogEvent& event) const
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
        buffer->AppendString(AsStringBuf("Unexpected error: "));
    }
    FormatMessage(buffer, TStringBuf(event.Message.Begin(), event.Message.End()));
    buffer->AppendChar('\t');

    if (event.ThreadId != NConcurrency::InvalidThreadId) {
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
    WriteFormatted(outputStream, GetStartLogEvent());
}

void TPlainTextLogFormatter::WriteLogSkippedEvent(IOutputStream* outputStream, i64 count, const TString& skippedBy) const
{
    WriteFormatted(outputStream, GetSkippedLogEvent(count, skippedBy));
}

////////////////////////////////////////////////////////////////////////////////

TJsonLogFormatter::TJsonLogFormatter(const THashMap<TString, NYTree::INodePtr>& commonFields)
    : CachingDateFormatter_(std::make_unique<TCachingDateFormatter>())
    , CommonFields_(commonFields)
{ }

size_t TJsonLogFormatter::WriteFormatted(IOutputStream* stream, const TLogEvent& event) const
{
    if (!stream) {
        return 0;
    }

    auto counterStream = TCountingOutput(stream);

    auto jsonConsumer = NJson::CreateJsonConsumer(&counterStream);
    NYTree::BuildYsonFluently(jsonConsumer.get())
        .BeginMap()
            .DoFor(CommonFields_, [] (auto fluent, auto item) {
                fluent.Item(item.first).Value(item.second);
            })
            .Items(event.StructuredMessage)
            .Item("instant").Value(ToString(CachingDateFormatter_->Format(event.Instant)))
            .Item("level").Value(FormatEnum(event.Level))
            .Item("category").Value(event.Category->Name)
        .EndMap();
    jsonConsumer->Flush();

    counterStream.Write('\n');

    return counterStream.Counter();
}

void TJsonLogFormatter::WriteLogReopenSeparator(IOutputStream* outputStream) const
{ }

void TJsonLogFormatter::WriteLogStartEvent(IOutputStream* outputStream) const
{
    WriteFormatted(outputStream, GetStartLogStructuredEvent());
}

void TJsonLogFormatter::WriteLogSkippedEvent(IOutputStream* outputStream, i64 count, const TString& skippedBy) const
{
    WriteFormatted(outputStream, GetSkippedLogStructuredEvent(count, skippedBy));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging

