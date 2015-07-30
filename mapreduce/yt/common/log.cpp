#include "log.h"

#include <util/datetime/base.h>
#include <util/system/mutex.h>
#include <util/stream/str.h>
#include <util/stream/printf.h>

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

class TStdErrLogger
    : public ILogger
{
public:
    TStdErrLogger(ELevel cutLevel)
        : CutLevel_(cutLevel)
    { }

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
        Cerr << stream.Str();
    }

private:
    ELevel CutLevel_;
    TMutex Mutex_;
};

ILoggerPtr CreateStdErrLogger(ILogger::ELevel cutLevel)
{
    return new TStdErrLogger(cutLevel);
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

