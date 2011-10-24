#pragma once

#include "common.h"
#include "pattern.h"

#include "../misc/ref_counted_base.h"

#include <util/system/file.h>
#include <util/stream/file.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

extern const char* const SystemLoggingCategory;

////////////////////////////////////////////////////////////////////////////////

struct ILogWriter
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<ILogWriter> TPtr;

    virtual void Write(const TLogEvent& event) = 0;
    virtual void Flush() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TStreamLogWriter
    : public ILogWriter
{
public:
    TStreamLogWriter(
        TOutputStream* stream,
        Stroka pattern);

    virtual void Write(const TLogEvent& event);
    virtual void Flush();

private:
    TOutputStream* Stream;
    Stroka Pattern;

};

////////////////////////////////////////////////////////////////////////////////

class TStdErrLogWriter
    : public TStreamLogWriter
{
public:
    TStdErrLogWriter(Stroka pattern);

};

////////////////////////////////////////////////////////////////////////////////

class TStdOutLogWriter
    : public TStreamLogWriter
{
public:
    TStdOutLogWriter(Stroka pattern);

};

////////////////////////////////////////////////////////////////////////////////

class TFileLogWriter
    : public ILogWriter
{
public:
    TFileLogWriter(
        Stroka fileName,
        Stroka pattern);

    virtual void Write(const TLogEvent& event);
    virtual void Flush();

private:
    static const size_t BufferSize = 1 << 16;

    void EnsureInitialized();

    Stroka FileName;
    Stroka Pattern;
    bool Initialized;
    THolder<TFile> File;
    THolder<TBufferedFileOutput> FileOutput;
    TFileLogWriter::TPtr LogWriter;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
