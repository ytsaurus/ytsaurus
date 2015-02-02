#pragma once

#include "common.h"
#include "config.h"
#include "pattern.h"

#include <core/misc/ref_counted.h>
#include <core/ytree/yson_serializable.h>

#include <util/system/file.h>
#include <util/stream/file.h>

#include <atomic>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

extern const char* const SystemLoggingCategory;

////////////////////////////////////////////////////////////////////////////////

struct ILogWriter
    : public virtual TRefCounted
{
    virtual void Write(const TLogEvent& event) = 0;
    virtual void Flush() = 0;
    virtual void Reload() = 0;
    virtual void CheckSpace(i64 minSpace) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TStreamLogWriter
    : public ILogWriter
{
public:
    explicit TStreamLogWriter(TOutputStream* stream = nullptr);
    ~TStreamLogWriter();

    virtual void Write(const TLogEvent& event) override;
    virtual void Flush() override;
    virtual void Reload() override;
    virtual void CheckSpace(i64 minSpace) override;

protected:
    TOutputStream* Stream_;

private:
    class TDateFormatter;

    std::unique_ptr<TMessageBuffer> Buffer_;
    std::unique_ptr<TDateFormatter> DateFormatter_;
};

////////////////////////////////////////////////////////////////////////////////

class TStderrLogWriter
    : public TStreamLogWriter
{
public:
    TStderrLogWriter();
};

////////////////////////////////////////////////////////////////////////////////

class TStdoutLogWriter
    : public TStreamLogWriter
{
public:
    TStdoutLogWriter();
};

////////////////////////////////////////////////////////////////////////////////

class TFileLogWriter
    : public TStreamLogWriter
{
public:
    explicit TFileLogWriter(const Stroka& fileName);
    virtual void Write(const TLogEvent& event) override;
    virtual void Flush() override;
    virtual void Reload() override;
    virtual void CheckSpace(i64 minSpace) override;

protected:
    void ReopenFile();
    void EnsureInitialized(bool writeTrailingNewline = false);

    Stroka FileName_;
    bool Initialized_;

    std::atomic<bool> Disabled_;

    std::unique_ptr<TFile> File_;
    std::unique_ptr<TBufferedFileOutput> FileOutput_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
