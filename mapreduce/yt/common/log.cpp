#include "log.h"

#include <util/datetime/base.h>
#include <util/system/mutex.h>
#include <util/stream/str.h>
#include <util/stream/printf.h>
#include <util/stream/file.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TNullLogger
    : public ILogger
{
public:
    void Log(ELevel level, const char* format, va_list args) override
    {
        UNUSED(level);
        UNUSED(format);
        UNUSED(args);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TLoggerBase
    : public ILogger
{
public:
    TLoggerBase(ELevel cutLevel)
        : CutLevel_(cutLevel)
    { }

    virtual void OutputLine(const Stroka& line) = 0;

    void Log(ELevel level, const char* format, va_list args) override
    {
        if (level > CutLevel_) {
            return;
        }

        TStringStream stream;
        stream << TInstant::Now() << " ";
        Printf(stream, format, args);
        stream << Endl;

        TGuard<TMutex> guard(Mutex_);
        OutputLine(stream.Str());
    }

private:
    ELevel CutLevel_;
    TMutex Mutex_;
};

////////////////////////////////////////////////////////////////////////////////

class TStdErrLogger
    : public TLoggerBase
{
public:
    TStdErrLogger(ELevel cutLevel)
        : TLoggerBase(cutLevel)
    { }

    void OutputLine(const Stroka& line) override
    {
        Cerr << line;
    }
};

ILoggerPtr CreateStdErrLogger(ILogger::ELevel cutLevel)
{
    return new TStdErrLogger(cutLevel);
}

////////////////////////////////////////////////////////////////////////////////

class TFileLogger
    : public TLoggerBase
{
public:
    TFileLogger(ELevel cutLevel, const Stroka& path)
        : TLoggerBase(cutLevel)
        , Stream_(path)
    { }

    void OutputLine(const Stroka& line) override
    {
        Stream_ << line;
    }

private:
    TFileOutput Stream_;
};

ILoggerPtr CreateFileLogger(ILogger::ELevel cutLevel, const Stroka& path)
{
    return new TFileLogger(cutLevel, path);
}

////////////////////////////////////////////////////////////////////////////////

static ILoggerPtr Logger;

struct TLoggerInitializer
{
    TLoggerInitializer()
    {
        Logger = new TNullLogger;
    }
} LoggerInitializer;

void SetLogger(ILoggerPtr logger)
{
    if (logger) {
        Logger = logger;
    } else {
        Logger = new TNullLogger;
    }
}

ILoggerPtr GetLogger()
{
    return Logger;
}

////////////////////////////////////////////////////////////////////////////////

}

