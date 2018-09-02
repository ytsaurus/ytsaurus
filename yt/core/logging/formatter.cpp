#include "formatter.h"
#include "private.h"
#include "log.h"
#include "log_manager.h"

#include <yt/build/build.h>

#include <yt/core/json/json_writer.h>
#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NLogging {

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
    event.Message = Format("Logging started (Version: %v, BuildHost: %v, BuildTime: %v)",
        GetVersion(),
        GetBuildHost(),
        GetBuildTime());
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

void TPlainTextLogFormatter::WriteFormatted(IOutputStream* outputStream, const TLogEvent& event) const
{
    if (!outputStream) {
        return;
    }

    TMessageBuffer* buffer = Buffer_.get();
    buffer->Reset();

    buffer->AppendString(CachingDateFormatter_->Format(event.Instant));
    buffer->AppendChar('\t');

    FormatLevel(buffer, event.Level);
    buffer->AppendChar('\t');

    buffer->AppendString(event.Category->Name);
    buffer->AppendChar('\t');

    FormatMessage(buffer, event.Message);
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
        buffer->AppendNumber(event.TraceId, 16);
    }
    buffer->AppendChar('\n');

    outputStream->Write(buffer->GetData(), buffer->GetBytesWritten());
}

void TPlainTextLogFormatter::WriteLogStartEvent(IOutputStream* outputStream) const
{
    WriteFormatted(outputStream, GetStartLogEvent());
}

////////////////////////////////////////////////////////////////////////////////

TJsonLogFormatter::TJsonLogFormatter()
    : CachingDateFormatter_(std::make_unique<TCachingDateFormatter>())
{ }

void TJsonLogFormatter::WriteFormatted(IOutputStream* stream, const TLogEvent& event) const
{
    if (!stream) {
        return;
    }

    auto jsonConsumer = NJson::CreateJsonConsumer(stream);
    NYTree::BuildYsonFluently(jsonConsumer.get())
        .BeginMap()
            .Items(event.StructuredMessage)
            .Item("instant").Value(ToString(CachingDateFormatter_->Format(event.Instant)))
            .Item("level").Value(FormatEnum(event.Level))
            .Item("category").Value(event.Category->Name)
        .EndMap();
    jsonConsumer->Flush();

    stream->Write('\n');
}

void TJsonLogFormatter::WriteLogStartEvent(IOutputStream* outputStream) const
{
    WriteFormatted(outputStream, GetStartLogStructuredEvent());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLogging
} // namespace NYT
