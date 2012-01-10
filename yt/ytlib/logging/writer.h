#pragma once

#include "common.h"
#include "pattern.h"

#include <ytlib/misc/ref_counted_base.h>
#include <ytlib/misc/configurable.h>

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

    DECLARE_ENUM(EType,
        (File)
        (StdOut)
        (StdErr)
    );

    struct TConfig
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        EType Type;
        Stroka Pattern;
        Stroka FileName;

        TConfig()
        {
            Register("type", Type);
            Register("pattern", Pattern)
                .CheckThat(~FromFunctor([] (const Stroka& pattern)
                {
                    Stroka errorMessage;
                    if (!ValidatePattern(pattern, &errorMessage))
                        ythrow yexception() << errorMessage;
                }));
            Register("file_name", FileName).Default();
        }

        virtual void Validate(const NYTree::TYPath& path) const
        {
            TConfigurable::Validate(path);

            if (Type == EType::File && FileName.empty()) {
                ythrow yexception() <<
                    Sprintf("FileName is empty while type is File (Path: %s)",
                        ~path);
            } else if (Type != EType::File && !FileName.empty()) {
                ythrow yexception() <<
                    Sprintf("FileName is not empty while type is not File (Path: %s)",
                        ~path);
            }
        }
    };
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
