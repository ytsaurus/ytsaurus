#include "pattern.h"

#include "../misc/foreach.h"

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

static Stroka FormatDateTime(TInstant dateTime)
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

static Stroka FormatLevel(ELogLevel level)
{
    switch (level)
    {
        case ELogLevel::Debug:   return "D";
        case ELogLevel::Info:    return "I";
        case ELogLevel::Warning: return "W";
        case ELogLevel::Error:   return "E";
        case ELogLevel::Fatal:   return "F";
        default: YASSERT(false); return "?";
    }
}

static void SetupFormatter(TPatternFormatter& formatter, const TLogEvent& event)
{
    formatter.AddProperty("level", FormatLevel(event.GetLevel()));
    formatter.AddProperty("datetime", FormatDateTime(event.GetDateTime()));
    formatter.AddProperty("message", event.GetMessage());
    formatter.AddProperty("category", event.GetCategory());
    formatter.AddProperty("tab", "\t");

    FOREACH(const auto& pair, event.GetProperties()) {
        formatter.AddProperty(pair.first, pair.second);
    }
}

Stroka FormatEvent(const TLogEvent& event, Stroka pattern)
{
    TPatternFormatter formatter;
    SetupFormatter(formatter, event);
    return formatter.Format(pattern);
}

bool ValidatePattern(Stroka pattern, Stroka* errorMessage)
{
    TPatternFormatter formatter;

    formatter.AddProperty("level", "");
    formatter.AddProperty("datetime", "");
    formatter.AddProperty("message", "");
    formatter.AddProperty("category", "");
    formatter.AddProperty("tab", "");

    try {
        formatter.Format(pattern);
    } catch (const yexception& e) {
        *errorMessage = e.what();
        return false;
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
