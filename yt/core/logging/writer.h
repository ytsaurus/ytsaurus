#pragma once

#include "config.h"
#include "pattern.h"

#include <yt/core/misc/ref_counted.h>

#include <yt/core/ytree/yson_serializable.h>

#include <util/stream/file.h>

#include <util/system/file.h>

#include <atomic>

namespace NYT {
namespace NLogging {

////////////////////////////////////////////////////////////////////////////////

struct ILogWriter
    : public virtual TRefCounted
{
    virtual void Write(const TLogEvent& event) = 0;
    virtual void Flush() = 0;
    virtual void Reload() = 0;
    virtual void CheckSpace(i64 minSpace) = 0;
};

DECLARE_REFCOUNTED_STRUCT(ILogWriter)
DEFINE_REFCOUNTED_TYPE(ILogWriter)

////////////////////////////////////////////////////////////////////////////////

class TStreamLogWriterBase
    : public ILogWriter
{
public:
    TStreamLogWriterBase();
    ~TStreamLogWriterBase();

    virtual void Write(const TLogEvent& event) override;
    virtual void Flush() override;
    virtual void Reload() override;
    virtual void CheckSpace(i64 minSpace) override;

protected:
    virtual IOutputStream* GetOutputStream() const noexcept = 0;
    virtual void OnException(const std::exception& ex);

private:
    class TCachingDateFormatter;

    std::unique_ptr<TMessageBuffer> Buffer_;
    std::unique_ptr<TCachingDateFormatter> CachingDateFormatter_;
};

////////////////////////////////////////////////////////////////////////////////

class TStreamLogWriter final
    : public TStreamLogWriterBase
{
public:
    explicit TStreamLogWriter(IOutputStream* stream)
        : Stream_(stream)
    { }

private:
    virtual IOutputStream* GetOutputStream() const noexcept override;

    IOutputStream* Stream_;
};

////////////////////////////////////////////////////////////////////////////////

class TStderrLogWriter final
    : public TStreamLogWriterBase
{
private:
    virtual IOutputStream* GetOutputStream() const noexcept override;
};

////////////////////////////////////////////////////////////////////////////////

class TStdoutLogWriter final
    : public TStreamLogWriterBase
{
private:
    virtual IOutputStream* GetOutputStream() const noexcept override;
};

////////////////////////////////////////////////////////////////////////////////

class TFileLogWriter final
    : public TStreamLogWriterBase
{
public:
    explicit TFileLogWriter(const TString& fileName);
    ~TFileLogWriter();

    virtual void Reload() override;
    virtual void CheckSpace(i64 minSpace) override;

private:
    virtual IOutputStream* GetOutputStream() const noexcept override;
    virtual void OnException(const std::exception& ex) override;

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
