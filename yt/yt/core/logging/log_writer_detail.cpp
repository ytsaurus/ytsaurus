#include "log_writer_detail.h"

#include "log.h"
#include "formatter.h"

#include <library/cpp/yt/system/handle_eintr.h>

namespace NYT::NLogging {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto RateLimitUpdatePeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

TRateLimitCounter::TRateLimitCounter(
    std::optional<i64> rateLimit,
    NProfiling::TCounter bytesCounter,
    NProfiling::TCounter skippedEventsCounter)
    : RateLimit_(rateLimit)
    , BytesCounter_(std::move(bytesCounter))
    , SkippedEventsCounter_(std::move(skippedEventsCounter))
    , LastUpdate_(TInstant::Now())
{ }

void TRateLimitCounter::SetRateLimit(std::optional<i64> rateLimit)
{
    RateLimit_ = rateLimit;
    LastUpdate_ = TInstant::Now();
    BytesWritten_ = 0;
}

bool TRateLimitCounter::IsLimitReached()
{
    if (!RateLimit_) {
        return false;
    }

    if (BytesWritten_ >= *RateLimit_) {
        SkippedEvents_++;
        SkippedEventsCounter_.Increment();
        return true;
    } else {
        return false;
    }
}

bool TRateLimitCounter::IsIntervalPassed()
{
    auto now = TInstant::Now();
    if (now - LastUpdate_ >= RateLimitUpdatePeriod) {
        LastUpdate_ = now;
        BytesWritten_ = 0;
        return true;
    }
    return false;
}

void TRateLimitCounter::UpdateCounter(i64 bytesWritten)
{
    BytesWritten_ += bytesWritten;
    BytesCounter_.Increment(bytesWritten);
}

i64 TRateLimitCounter::GetAndResetLastSkippedEventsCount()
{
    i64 old = SkippedEvents_;
    SkippedEvents_ = 0;
    return old;
}

////////////////////////////////////////////////////////////////////////////////

TSegmentSizeReporter::TSegmentSizeReporter(TString logWriterName)
    : CurrentSegmentSizeGauge_(
        TProfiler{"/logging"}.WithSparse().WithTag("writer", std::move(logWriterName)).Gauge("/current_segment_size"))
{ }

void TSegmentSizeReporter::IncrementSegmentSize(i64 size)
{
    CurrentSegmentSizeGauge_.Update(CurrentSegmentSize_.fetch_add(size, std::memory_order::relaxed) + size);
}

void TSegmentSizeReporter::ResetSegmentSize(i64 size)
{
    CurrentSegmentSize_.store(size, std::memory_order::relaxed);
    CurrentSegmentSizeGauge_.Update(size);
}

////////////////////////////////////////////////////////////////////////////////

TRateLimitingLogWriterBase::TRateLimitingLogWriterBase(TCallback<i64(const TLogEvent&)> writeImpl, TString name, TLogWriterConfigPtr config)
    : WriteImpl_(writeImpl)
    , Name_(std::move(name))
    , Config_(std::move(config))
    , RateLimit_(
        std::nullopt,
        {},
        TProfiler{"/logging"}.WithSparse().WithTag("writer", Name_).Counter("/events_skipped_by_global_limit"))
{ }

void TRateLimitingLogWriterBase::Write(const TLogEvent& event)
{
    auto* categoryRateLimit = GetCategoryRateLimitCounter(event.Category->Name);
    if (RateLimit_.IsIntervalPassed()) {
        auto eventsSkipped = RateLimit_.GetAndResetLastSkippedEventsCount();
        WriteLogSkippedEvent(eventsSkipped, Name_);
    }
    if (categoryRateLimit->IsIntervalPassed()) {
        auto eventsSkipped = categoryRateLimit->GetAndResetLastSkippedEventsCount();
        WriteLogSkippedEvent(eventsSkipped, event.Category->Name);
    }

    // We do not do this in the constructor since it happens to be called during TLogManager construction itself.
    if (!SystemCategory_) {
        SystemCategory_ = GetDefaultLogManager()->GetCategory(SystemLoggingCategoryName);
    }
    bool isSystemEvent = event.Category == SystemCategory_;

    // We always let system events pass through without affecting rate limits.
    if (isSystemEvent || (!RateLimit_.IsLimitReached() && !categoryRateLimit->IsLimitReached())) {
        auto bytesWritten = WriteImpl_(event);

        if (!isSystemEvent) {
            RateLimit_.UpdateCounter(bytesWritten);
            categoryRateLimit->UpdateCounter(bytesWritten);
        }
    }
}

void TRateLimitingLogWriterBase::SetRateLimit(std::optional<i64> limit)
{
    RateLimit_.SetRateLimit(limit);
}

void TRateLimitingLogWriterBase::SetCategoryRateLimits(const THashMap<TString, i64>& categoryRateLimits)
{
    CategoryToRateLimit_.clear();
    for (const auto& it : categoryRateLimits) {
        GetCategoryRateLimitCounter(it.first)->SetRateLimit(it.second);
    }
}

void TRateLimitingLogWriterBase::WriteLogSkippedEvent(i64 eventsSkipped, TStringBuf skippedBy)
{
    if (eventsSkipped > 0 && Config_->AreSystemMessagesEnabled()) {
        WriteImpl_(GetSkippedLogEvent(Config_->GetSystemMessageFamily(), eventsSkipped, skippedBy));
    }
}

TRateLimitCounter* TRateLimitingLogWriterBase::GetCategoryRateLimitCounter(TStringBuf category)
{
    auto it = CategoryToRateLimit_.find(category);
    if (it == CategoryToRateLimit_.end()) {
        auto r = TProfiler{"/logging"}
            .WithSparse()
            .WithTag("writer", Name_)
            .WithTag("category", TString{category}, -1);

        // TODO(prime@): optimize sensor count
        it = CategoryToRateLimit_.insert({category, TRateLimitCounter(
            std::nullopt,
            r.Counter("/bytes_written"),
            r.Counter("/events_skipped_by_category_limit"))}).first;
    }
    return &it->second;
}

////////////////////////////////////////////////////////////////////////////////

TStreamLogWriterBase::TStreamLogWriterBase(
    std::unique_ptr<ILogFormatter> formatter,
    const TString& name,
    TLogWriterConfigPtr config)
    : TRateLimitingLogWriterBase(BIND(&TStreamLogWriterBase::WriteImpl, Unretained(this)), name, std::move(config))
    , TSegmentSizeReporter(name)
    , Formatter_(std::move(formatter))
{ }

i64 TStreamLogWriterBase::WriteImpl(const TLogEvent& event)
{
    auto* stream = GetOutputStream();
    if (!stream) {
        return 0;
    }

    try {
        auto bytesWritten = Formatter_->WriteFormatted(stream, event);
        IncrementSegmentSize(bytesWritten);
        return bytesWritten;
    } catch (const std::exception& ex) {
        OnException(ex);
    }

    return 0;
}

void TStreamLogWriterBase::Flush()
{
    auto* stream = GetOutputStream();
    if (!stream) {
        return;
    }

    try {
        stream->Flush();
    } catch (const std::exception& ex) {
        OnException(ex);
    }
}

void TStreamLogWriterBase::Reload()
{ }

void TStreamLogWriterBase::OnException(const std::exception& ex)
{
    // Fail with drama by default.
    TRawFormatter<1024> formatter;
    formatter.AppendString("\n*** Unhandled exception in log writer: ");
    formatter.AppendString(ex.what());
    formatter.AppendString("\n*** Aborting ***\n");
#ifdef _unix_
    HandleEintr(::write, 2, formatter.GetData(), formatter.GetBytesWritten());
#else
    ::WriteFile(
        GetStdHandle(STD_ERROR_HANDLE),
        formatter.GetData(),
        formatter.GetBytesWritten(),
        /*lpNumberOfBytesWritten*/ nullptr,
        /*lpOverlapped*/ nullptr);
#endif
    _exit(100);
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
