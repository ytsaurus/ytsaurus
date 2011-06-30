#pragma once

#include "common.h"

#include <util/system/file.h>
#include <util/stream/file.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

extern const char* const SystemLoggingCategory;

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ELogLevel,
    (Minimum)
    (Debug)
    (Info)
    (Warning)
    (Error)
    (Fatal)
    (Maximum)
);

class TLogEvent
{
public:
    typedef TPair<Stroka, Stroka> TProperty;
    typedef yvector<TProperty> TProperties;

    TLogEvent(Stroka category, ELogLevel level, Stroka message);

    void AddProperty(Stroka name, Stroka value);

    Stroka GetCategory() const;
    ELogLevel GetLevel() const;
    Stroka GetMessage() const;
    TInstant GetDateTime() const;
    const TProperties& GetProperties() const;

private:
    Stroka Category;
    ELogLevel Level;
    Stroka Message;
    TInstant DateTime;
    TProperties Properties;

};

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
