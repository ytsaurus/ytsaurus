#pragma once

#include "private.h"
#include "log_writer.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/actions/callback.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

class TRateLimitCounter
{
public:
    TRateLimitCounter(
        std::optional<i64> rateLimit,
        NProfiling::TCounter bytesCounter,
        NProfiling::TCounter skippedEventsCounter);

    void SetRateLimit(std::optional<i64> rateLimit);
    bool IsLimitReached();
    bool IsIntervalPassed();
    i64 GetAndResetLastSkippedEventsCount();

    void UpdateCounter(i64 bytesWritten);

private:
    std::optional<i64> RateLimit_;

    NProfiling::TCounter BytesCounter_;
    NProfiling::TCounter SkippedEventsCounter_;

    i64 BytesWritten_ = 0;
    i64 SkippedEvents_ = 0;

    TInstant LastUpdate_;
};

////////////////////////////////////////////////////////////////////////////////

//! A mix-in thread-safe class intended to report the size of the current log segment.
class TSegmentSizeReporter
{
public:
    TSegmentSizeReporter(TString logWriterName);

    void IncrementSegmentSize(i64 size);
    void ResetSegmentSize(i64 size);

private:
    std::atomic<i64> CurrentSegmentSize_{0};
    NProfiling::TGauge CurrentSegmentSizeGauge_;
};

////////////////////////////////////////////////////////////////////////////////

//! Implements the rate-limiting part of ILogWriter.
//! System log events are always allowed to pass through.
//! Requires an underlying Write implementation to be provided upon construction.
//!
//! NB(achulkov2): The writeImpl solution is a bit sad, but I see no other way to work
//! around the dynamically interpreted ILogWriter/IFileLogWriter interface. Ideally, we
//! need to get rid of dynamic casts in TLogManager by abstracting the file-writer related
//! logic. Then we could switch to a wrapper-like solution, similar to our channels.
class TRateLimitingLogWriterBase
    : public virtual ILogWriter
{
public:
    //! The return value of the callback corresponds to the number of bytes logged by writing this event.
    TRateLimitingLogWriterBase(
        TCallback<i64(const TLogEvent&)> writeImpl,
        TString name,
        TLogWriterConfigPtr config);

    //! Can produce zero or multiple synchronous calls to the underlying write implementation.
    void Write(const TLogEvent& event) override;

    // There are no overrides for Reload and Flush intentionally.
    // You need to implement them yourself.

    void SetRateLimit(std::optional<i64> limit) override;

    void SetCategoryRateLimits(const THashMap<TString, i64>& categoryRateLimits) override;

private:
    const TCallback<i64(const TLogEvent&)> WriteImpl_;
    const TString Name_;
    const TLogWriterConfigPtr Config_;
    const TLoggingCategory* SystemCategory_ = nullptr;

    // These fields are only accessed via the main logging thread and during
    // log manager's Initialize call, so the lack of synchronization should be fine.
    TRateLimitCounter RateLimit_;
    THashMap<TStringBuf, TRateLimitCounter> CategoryToRateLimit_;

    void WriteLogSkippedEvent(i64 eventsSkipped, TStringBuf skippedBy);

    TRateLimitCounter* GetCategoryRateLimitCounter(TStringBuf category);
};

////////////////////////////////////////////////////////////////////////////////

class TStreamLogWriterBase
    : public virtual ILogWriter
    , public TRateLimitingLogWriterBase
    , public TSegmentSizeReporter
{
public:
    TStreamLogWriterBase(
        std::unique_ptr<ILogFormatter> formatter,
        const TString& name,
        TLogWriterConfigPtr config);

    i64 WriteImpl(const TLogEvent& event);

    void Flush() override;
    void Reload() override;

protected:
    virtual IOutputStream* GetOutputStream() const noexcept = 0;
    virtual void OnException(const std::exception& ex);

    const std::unique_ptr<ILogFormatter> Formatter_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
