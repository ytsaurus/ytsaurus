#pragma once

#include "common.h"
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

    DECLARE_ENUM(EType,
        (File)
        (Stdout)
        (Stderr)
        (Raw)
    );

    struct TConfig
        : public TYsonSerializable
    {
        EType Type;
        Stroka Pattern;
        Stroka FileName;

        TConfig()
        {
            RegisterParameter("type", Type);
            RegisterParameter("pattern", Pattern)
                .Default()
                .CheckThat(BIND([] (const Stroka& pattern) {
                    auto error = ValidatePattern(pattern);
                    if (!error.IsOK()) {
                        THROW_ERROR error;
                    }
                }));
            RegisterParameter("file_name", FileName)
                .Default();

            RegisterValidator([&] () {
                DoValidate();
            });
        }

        void DoValidate()
        {
            if ((Type == EType::File || Type == EType::Raw) && FileName.empty()) {
                THROW_ERROR_EXCEPTION("FileName is empty while type is File");
            } else if (Type != EType::File && Type != EType::Raw && !FileName.empty()) {
                THROW_ERROR_EXCEPTION("FileName is not empty while type is not File");
            }

            if (Type != EType::Raw && Pattern.empty()) {
                THROW_ERROR_EXCEPTION("Pattern is empty while type is not Raw");
            }
        }
    };
    
    typedef TIntrusivePtr<TConfig> TConfigPtr;

};

////////////////////////////////////////////////////////////////////////////////

class TStreamLogWriter
    : public ILogWriter
{
public:
    TStreamLogWriter(
        TOutputStream* stream,
        Stroka pattern);

    virtual void Write(const TLogEvent& event) override;
    virtual void Flush() override;
    virtual void Reload() override;
    virtual void CheckSpace(i64 minSpace) override;

private:
    TOutputStream* Stream;
    Stroka Pattern;

};

////////////////////////////////////////////////////////////////////////////////

class TStderrLogWriter
    : public TStreamLogWriter
{
public:
    explicit TStderrLogWriter(const Stroka& pattern) ;

};

////////////////////////////////////////////////////////////////////////////////

class TStdoutLogWriter
    : public TStreamLogWriter
{
public:
    explicit TStdoutLogWriter(const Stroka& pattern);

};

////////////////////////////////////////////////////////////////////////////////

class TFileLogWriterBase
    : public ILogWriter
{
public:
    explicit TFileLogWriterBase(const Stroka& fileName);

    virtual void CheckSpace(i64 minSpace) override;

protected:
    static const size_t BufferSize = 1 << 16;

    void ReopenFile();

    Stroka FileName;
    bool Initialized;

    TAtomic NotEnoughSpace;

    std::unique_ptr<TFile> File;
    std::unique_ptr<TBufferedFileOutput> FileOutput;
};

////////////////////////////////////////////////////////////////////////////////

class TFileLogWriter
    : public TFileLogWriterBase
{
public:
    TFileLogWriter(
        Stroka fileName,
        Stroka pattern);

    virtual void Write(const TLogEvent& event) override;
    virtual void Flush() override;
    virtual void Reload() override;

private:

    void EnsureInitialized();

    Stroka Pattern;
    ILogWriterPtr LogWriter;

};

////////////////////////////////////////////////////////////////////////////////

class TRawFileLogWriter
    : public TFileLogWriterBase
{
public:
    explicit TRawFileLogWriter(const Stroka& fileName);

    virtual void Write(const TLogEvent& event);
    virtual void Flush();
    virtual void Reload();

private:

    void EnsureInitialized();

    std::unique_ptr<TMessageBuffer> Buffer;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
