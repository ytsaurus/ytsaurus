#pragma once

#include "config.h"
#include "formatter.h"

#include <yt/yt/core/misc/ref_counted.h>

#include <yt/yt/core/ytree/yson_serializable.h>

#include <yt/yt/library/profiling/sensor.h>

#include <util/stream/file.h>
#include <util/stream/output.h>

#include <atomic>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

class TRateLimitCounter
{
public:
    TRateLimitCounter(
        std::optional<size_t> limit,
        NProfiling::TCounter bytesCounter,
        NProfiling::TCounter skippedEventsCounter);

    void SetRateLimit(std::optional<size_t> rateLimit);
    bool IsLimitReached();
    bool IsIntervalPassed();
    i64 GetAndResetLastSkippedEventsCount();

    void UpdateCounter(size_t bytesWritten);

private:
    size_t BytesWritten_ = 0;
    i64 SkippedEvents_ = 0;
    TDuration UpdatePeriod_ = TDuration::Seconds(1);
    TInstant LastUpdate_;
    std::optional<size_t> RateLimit_;
    NProfiling::TCounter BytesCounter_;
    NProfiling::TCounter SkippedEventsCounter_;
};

////////////////////////////////////////////////////////////////////////////////

struct ILogWriter
    : public virtual TRefCounted
{
    virtual void Write(const TLogEvent& event) = 0;
    virtual void Flush() = 0;
    virtual void Reload() = 0;
    virtual void CheckSpace(i64 minSpace) = 0;
    virtual void SetRateLimit(std::optional<size_t> limit) = 0;
    virtual void SetCategoryRateLimits(const THashMap<TString, size_t>& categoryRateLimits) = 0;
};

DEFINE_REFCOUNTED_TYPE(ILogWriter)

////////////////////////////////////////////////////////////////////////////////

class TStreamLogWriterBase
    : public ILogWriter
{
public:
    TStreamLogWriterBase(std::unique_ptr<ILogFormatter> formatter, TString name);
    ~TStreamLogWriterBase();

    void Write(const TLogEvent& event) override;
    void Flush() override;
    void Reload() override;
    void CheckSpace(i64 minSpace) override;

    void SetRateLimit(std::optional<size_t> limit) override;
    void SetCategoryRateLimits(const THashMap<TString, size_t>& categoryRateLimits) override;

protected:
    virtual IOutputStream* GetOutputStream() const noexcept = 0;
    virtual void OnException(const std::exception& ex);

    void ResetCurrentSegment(i64 size);

    TRateLimitCounter* GetCategoryRateLimitCounter(TStringBuf category);

    const std::unique_ptr<ILogFormatter> LogFormatter_;
    const TString Name_;

    TRateLimitCounter RateLimit_;
    THashMap<TStringBuf, TRateLimitCounter> CategoryToRateLimit_;

    i64 CurrentSegmentSize_ = 0;
    NProfiling::TGauge CurrentSegmentSizeGauge_;
};

////////////////////////////////////////////////////////////////////////////////

class TStreamLogWriter
    : public TStreamLogWriterBase
{
public:
    TStreamLogWriter(IOutputStream* stream, std::unique_ptr<ILogFormatter> formatter, TString name);

private:
    IOutputStream* GetOutputStream() const noexcept override;

    IOutputStream* const Stream_;
};

////////////////////////////////////////////////////////////////////////////////

class TStderrLogWriter
    : public TStreamLogWriterBase
{
public:
    using TStreamLogWriterBase::TStreamLogWriterBase;
    TStderrLogWriter();

private:
    IOutputStream* GetOutputStream() const noexcept override;
};

////////////////////////////////////////////////////////////////////////////////

class TStdoutLogWriter
    : public TStreamLogWriterBase
{
public:
    using TStreamLogWriterBase::TStreamLogWriterBase;

private:
    IOutputStream* GetOutputStream() const noexcept override;
};

////////////////////////////////////////////////////////////////////////////////

class TFileLogWriter
    : public TStreamLogWriterBase
{
public:
    TFileLogWriter(
        std::unique_ptr<ILogFormatter> formatter,
        TString writerName,
        TString fileName,
        IInvokerPtr compressionInvoker,
        bool enableCompression = false,
        ECompressionMethod compressionMethod = ECompressionMethod::Gzip,
        int compressionLevel = 6);
    ~TFileLogWriter();

    void Reload() override;
    void CheckSpace(i64 minSpace) override;

protected:
    IOutputStream* GetOutputStream() const noexcept override;
    void OnException(const std::exception& ex) override;

private:
    const TString FileName_;
    const IInvokerPtr CompressionInvoker_;
    const bool EnableCompression_;
    const ECompressionMethod CompressionMethod_;
    const int CompressionLevel_;

    std::atomic<bool> Disabled_ = false;

    std::unique_ptr<TFile> File_;
    IStreamLogOutputPtr OutputStream_;

    void Open();
    void Close();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
