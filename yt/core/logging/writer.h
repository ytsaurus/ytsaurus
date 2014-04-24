#pragma once

#include "common.h"
#include "config.h"
#include "pattern.h"

#include <core/misc/ref_counted.h>
#include <core/ytree/yson_serializable.h>

#include <util/system/file.h>
#include <util/stream/file.h>

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

    virtual void Write(const TLogEvent& event) override;
    virtual void Flush() override;
    virtual void Reload() override;
    virtual void CheckSpace(i64 minSpace) override;

protected:
    TOutputStream* Stream_;

private:
    std::unique_ptr<TMessageBuffer> Buffer_;
};

////////////////////////////////////////////////////////////////////////////////

class TStdErrLogWriter
    : public TStreamLogWriter
{
public:
    TStdErrLogWriter();
};

////////////////////////////////////////////////////////////////////////////////

class TStdOutLogWriter
    : public TStreamLogWriter
{
public:
    TStdOutLogWriter();
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
    static const size_t BufferSize = 1 << 16;

    void ReopenFile();
    void EnsureInitialized(bool trailingLinebreak = false);

    Stroka FileName_;
    bool Initialized_;

    TAtomic NotEnoughSpace_;

    std::unique_ptr<TFile> File_;
    std::unique_ptr<TBufferedFileOutput> FileOutput_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
