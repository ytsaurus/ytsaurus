#pragma once

#include "config.h"
#include "pattern.h"

#include <yt/core/misc/ref_counted.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

class TCachingDateFormatter
{
public:
    TCachingDateFormatter();

    const char* Format(NProfiling::TCpuInstant instant);

private:
    void Update(NProfiling::TCpuInstant instant);

    TMessageBuffer Cached_;
    NProfiling::TCpuInstant Deadline_ = 0;
    NProfiling::TCpuInstant Liveline_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ILogFormatter
{
    virtual ~ILogFormatter() = default;
    virtual size_t WriteFormatted(IOutputStream* outputStream, const TLogEvent& event) const = 0;
    virtual void WriteLogStartEvent(IOutputStream* outputStream) const = 0;
    virtual void WriteLogSkippedEvent(IOutputStream* outputStream, i64 count, const TString& skippedBy) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TPlainTextLogFormatter
    : public ILogFormatter
{
public:
    TPlainTextLogFormatter();
    virtual size_t WriteFormatted(IOutputStream* outputStream, const TLogEvent& event) const override;

    virtual void WriteLogStartEvent(IOutputStream* outputStream) const override;

    virtual void WriteLogSkippedEvent(IOutputStream* outputStream, i64 count, const TString& skippedBy) const override;

private:

    std::unique_ptr<TMessageBuffer> Buffer_;
    std::unique_ptr<TCachingDateFormatter> CachingDateFormatter_;
};

////////////////////////////////////////////////////////////////////////////////

class TJsonLogFormatter
    : public ILogFormatter
{
public:
    TJsonLogFormatter();

    virtual size_t WriteFormatted(IOutputStream* outputStream, const TLogEvent& event) const override;

    virtual void WriteLogStartEvent(IOutputStream* outputStream) const override;

    virtual void WriteLogSkippedEvent(IOutputStream* outputStream, i64 count, const TString& skippedBy) const override;

private:
    std::unique_ptr<TCachingDateFormatter> CachingDateFormatter_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
