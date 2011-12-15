#pragma once

#include "../misc/common.h"
#include "../misc/enum.h"
#include "../misc/thread.h"

#include <util/system/thread.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ELogLevel,
    (Minimum)
    (Trace)
    (Debug)
    (Info)
    (Warning)
    (Error)
    (Fatal)
    (Maximum)
);

const i32 InvalidLine = -1;

struct TLogEvent
{
    TLogEvent()
        : DateTime(TInstant::Now())
        , FileName(NULL)
        , Line(InvalidLine)
        , ThreadId(TThread::ImpossibleThreadId())
        , Function(NULL)
    { }

    TLogEvent(const Stroka& category, ELogLevel level, const Stroka& message)
        : Category(category)
        , Level(level)
        , Message(message)
        , DateTime(TInstant::Now())
        , FileName(NULL)
        , Line(InvalidLine)
        , ThreadId(TThread::ImpossibleThreadId())
    { }
    
    Stroka Category;
    ELogLevel Level;
    Stroka Message;
    TInstant DateTime;
    const char* FileName;
    i32 Line;
    NThread::TThreadId ThreadId;
    const char* Function;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
