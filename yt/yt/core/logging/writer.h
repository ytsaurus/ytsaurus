#pragma once

#include "config.h"
#include "formatter.h"

#include <yt/core/misc/ref_counted.h>

#include <yt/core/ytree/yson_serializable.h>

#include <yt/yt/library/profiling/sensor.h>

#include <util/stream/file.h>

#include <atomic>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

class TRateLimitCounter
{
public:
    TRateLimitCounter(
        std::optional<i64> limit,
        NProfiling::TCounter bytesCounter,
        NProfiling::TCounter skippedEventsCounter);

    void SetRateLimit(std::optional<i64> rateLimit);
    bool IsLimitReached();
    bool IsIntervalPassed();
    i64 GetAndResetLastSkippedEventsCount();

    void UpdateCounter(i64 bytesWritten);

private:
    i64 BytesWritten_ = 0;
    i64 SkippedEvents_ = 0;
    TInstant LastUpdate_;
    std::optional<i64> RateLimit_;
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
    virtual void SetRateLimit(std::optional<i64> limit) = 0;
    virtual void SetCategoryRateLimits(const THashMap<TString, i64>& categoryRateLimits) = 0;
};

DEFINE_REFCOUNTED_TYPE(ILogWriter)

////////////////////////////////////////////////////////////////////////////////

class TStreamLogWriterBase
    : public ILogWriter
{
public:
    TStreamLogWriterBase(std::unique_ptr<ILogFormatter> formatter, TString name);
    ~TStreamLogWriterBase();

    virtual void Write(const TLogEvent& event) override;
    virtual void Flush() override;
    virtual void Reload() override;
    virtual void CheckSpace(i64 minSpace) override;

    virtual void SetRateLimit(std::optional<i64> limit) override;
    virtual void SetCategoryRateLimits(const THashMap<TString, i64>& categoryRateLimits) override;

protected:
    virtual IOutputStream* GetOutputStream() const noexcept = 0;
    virtual void OnException(const std::exception& ex);

    TRateLimitCounter* GetCategoryRateLimitCounter(TStringBuf category);

    const std::unique_ptr<ILogFormatter> LogFormatter_;
    const TString Name_;

    TRateLimitCounter RateLimit_;
    THashMap<TStringBuf, TRateLimitCounter> CategoryToRateLimit_;
};

////////////////////////////////////////////////////////////////////////////////

class TStreamLogWriter
    : public TStreamLogWriterBase
{
public:
    TStreamLogWriter(IOutputStream* stream, std::unique_ptr<ILogFormatter> formatter, TString name);

private:
    virtual IOutputStream* GetOutputStream() const noexcept override;

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
    virtual IOutputStream* GetOutputStream() const noexcept override;
};

////////////////////////////////////////////////////////////////////////////////

class TStdoutLogWriter
    : public TStreamLogWriterBase
{
public:
    using TStreamLogWriterBase::TStreamLogWriterBase;

private:
    virtual IOutputStream* GetOutputStream() const noexcept override;
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
        bool enableCompression = false,
        size_t compressionLevel = 6);
    ~TFileLogWriter();

    virtual void Reload() override;
    virtual void CheckSpace(i64 minSpace) override;

protected:
    virtual IOutputStream* GetOutputStream() const noexcept override;
    virtual void OnException(const std::exception& ex) override;

private:
    const TString FileName_;
    const bool EnableCompression_;
    const size_t CompressionLevel_;

    std::atomic<bool> Disabled_ = false;

    std::unique_ptr<TFile> File_;
    std::unique_ptr<TFixedBufferFileOutput> FileOutput_;

    std::unique_ptr<TRandomAccessGZipFile> CompressedOutput_;

    void Open();
    void Close();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
