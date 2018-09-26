#pragma once

#include "config.h"
#include "formatter.h"

#include <yt/core/misc/ref_counted.h>

#include <yt/core/ytree/yson_serializable.h>

#include <util/stream/file.h>

#include <atomic>

namespace NYT {
namespace NLogging {

////////////////////////////////////////////////////////////////////////////////

class TRateLimitCounter
{
public:
    TRateLimitCounter(
        TNullable<size_t> limit,
        const NProfiling::TMonotonicCounter& bytesCounter,
        const NProfiling::TMonotonicCounter& skippedEventsCounter);

    void SetRateLimit(TNullable<size_t> rateLimit);
    void SetBytesCounter(const NProfiling::TMonotonicCounter& counter);
    void SetSkippedEventsCounter(const NProfiling::TMonotonicCounter& counter);
    bool IsLimitReached();
    bool UpdateTime();
    void UpdateCounter(size_t bytesWritten);
    i64 GetLastSkippedEventsCount();

protected:
    size_t BytesWritten_ = 0;
    i64 SkippedEvents_ = 0;
    TDuration UpdatePeriod_ = TDuration::Seconds(1);
    TInstant LastUpdate_;
    TNullable<size_t> RateLimit_;
    NProfiling::TMonotonicCounter BytesCounter_;
    NProfiling::TMonotonicCounter SkippedEventsCounter_;
};

////////////////////////////////////////////////////////////////////////////////

struct ILogWriter
    : public virtual TRefCounted
{
    virtual void Write(const TLogEvent& event) = 0;
    virtual void Flush() = 0;
    virtual void Reload() = 0;
    virtual void CheckSpace(i64 minSpace) = 0;
    virtual void SetRateLimit(TNullable<size_t> limit) = 0;
    virtual void SetCategoryRateLimits(const THashMap<TString, size_t>& categoryRateLimits) = 0;
};

DECLARE_REFCOUNTED_STRUCT(ILogWriter)
DEFINE_REFCOUNTED_TYPE(ILogWriter)

////////////////////////////////////////////////////////////////////////////////

class TStreamLogWriterBase
    : public ILogWriter
{
public:
    explicit TStreamLogWriterBase(std::unique_ptr<ILogFormatter> formatter, TString name);
    ~TStreamLogWriterBase();

    virtual void Write(const TLogEvent& event) override;
    virtual void Flush() override;
    virtual void Reload() override;
    virtual void CheckSpace(i64 minSpace) override;

    virtual void SetRateLimit(TNullable<size_t> limit) override;
    virtual void SetCategoryRateLimits(const THashMap<TString, size_t>& categoryRateLimits) override;

protected:
    virtual IOutputStream* GetOutputStream() const noexcept = 0;
    virtual void OnException(const std::exception& ex);

    TRateLimitCounter* GetCategoryRateLimitCounter(const TString& category);

    std::unique_ptr<ILogFormatter> LogFormatter;
    TString Name_;
    TRateLimitCounter RateLimit_;
    THashMap<TString, TRateLimitCounter> CategoryToRateLimit_;
};

////////////////////////////////////////////////////////////////////////////////

class TStreamLogWriter final
    : public TStreamLogWriterBase
{
public:
    explicit TStreamLogWriter(IOutputStream* stream, std::unique_ptr<ILogFormatter> formatter, TString name)
        : TStreamLogWriterBase::TStreamLogWriterBase(std::move(formatter), std::move(name))
        , Stream_(stream)
    { }

private:
    virtual IOutputStream* GetOutputStream() const noexcept override;

    IOutputStream* Stream_;
};

////////////////////////////////////////////////////////////////////////////////

class TStderrLogWriter final
    : public TStreamLogWriterBase
{
public:
    using TStreamLogWriterBase::TStreamLogWriterBase;
    TStderrLogWriter();

private:
    virtual IOutputStream* GetOutputStream() const noexcept override;
};

////////////////////////////////////////////////////////////////////////////////

class TStdoutLogWriter final
    : public TStreamLogWriterBase
{
public:
    using TStreamLogWriterBase::TStreamLogWriterBase;

private:
    virtual IOutputStream* GetOutputStream() const noexcept override;
};

////////////////////////////////////////////////////////////////////////////////

class TFileLogWriter final
    : public TStreamLogWriterBase
{
public:
    TFileLogWriter(std::unique_ptr<ILogFormatter> formatter, TString writerName, TString fileName);
    ~TFileLogWriter();

    virtual void Reload() override;
    virtual void CheckSpace(i64 minSpace) override;

protected:
    virtual IOutputStream* GetOutputStream() const noexcept override;
    virtual void OnException(const std::exception& ex) override;

private:
    void Open();
    void Close();

    TString FileName_;
    std::atomic<bool> Disabled_ = {false};

    std::unique_ptr<TFile> File_;
    std::unique_ptr<TFixedBufferFileOutput> FileOutput_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NLogging
} // namespace NYT
