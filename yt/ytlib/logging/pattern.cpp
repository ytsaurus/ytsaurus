#include "stdafx.h"
#include "pattern.h"

#include <ytlib/misc/foreach.h>
#include <ytlib/misc/fs.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

Stroka FormatDateTime(TInstant dateTime)
{
    timeval time1 = dateTime.TimeVal();
    tm time2;
    dateTime.LocalTime(&time2);
    return Sprintf("%04d-%02d-%02d %02d:%02d:%02d,%03d",
                   time2.tm_year + 1900,
                   time2.tm_mon + 1,
                   time2.tm_mday,
                   time2.tm_hour,
                   time2.tm_min,
                   time2.tm_sec,
                   (int) time1.tv_usec / 1000);
}

Stroka FormatLevel(ELogLevel level)
{
    switch (level)
    {
        case ELogLevel::Trace:   return "T";
        case ELogLevel::Debug:   return "D";
        case ELogLevel::Info:    return "I";
        case ELogLevel::Warning: return "W";
        case ELogLevel::Error:   return "E";
        case ELogLevel::Fatal:   return "F";
        default: YUNREACHABLE();
    }
}

Stroka FormatMessage(const Stroka& message)
{
    Stroka result;
    result.reserve(message.length() + 10);
    for (int index = 0; index < static_cast<int>(message.length()); ++index) {
        switch (message[index]) {
            case '\n':
                result.append("\\n");
                break;

            default:
                result.append(message[index]);
                break;
        }
    }
    return result;
}

static void SetupFormatter(TPatternFormatter& formatter, const TLogEvent& event)
{
    formatter.AddProperty("level", FormatLevel(event.Level));
    formatter.AddProperty("datetime", FormatDateTime(event.DateTime));
    formatter.AddProperty("message", FormatMessage(event.Message));
    formatter.AddProperty("category", event.Category);
    formatter.AddProperty("tab", "\t");
    if (event.FileName) {
        formatter.AddProperty("file", NFS::GetFileName(event.FileName));
    }
    if (event.Line != TLogEvent::InvalidLine) {
        formatter.AddProperty("line", ToString(event.Line));
    }
    if (event.ThreadId != NThread::InvalidThreadId) {
        formatter.AddProperty("thread", ToString(event.ThreadId));
    }
    if (event.Function) {
        formatter.AddProperty("function", event.Function);
    }
}

Stroka FormatEvent(const TLogEvent& event, Stroka pattern)
{
    TPatternFormatter formatter;
    SetupFormatter(formatter, event);
    return formatter.Format(pattern);
}

TError ValidatePattern(const Stroka& pattern)
{
    TPatternFormatter formatter;

    formatter.AddProperty("level", "");
    formatter.AddProperty("datetime", "");
    formatter.AddProperty("message", "");
    formatter.AddProperty("category", "");
    formatter.AddProperty("tab", "");

    try {
        formatter.Format(pattern);
    } catch (const std::exception& ex) {
        return TError(ex);
    }

    return TError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
