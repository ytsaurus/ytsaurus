#include "stdafx.h"
#include "pattern.h"

#include <core/misc/fs.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

namespace {

// Ultra-fast specialized versions of AppendNumber.
void AppendDigit(TMessageBuffer* out, int value)
{
    out->AppendChar('0' + value);
}

void AppendNumber2(TMessageBuffer* out, int value)
{
    AppendDigit(out, value / 10);
    AppendDigit(out, value % 10);
}

void AppendNumber3(TMessageBuffer* out, int value)
{
    AppendDigit(out, value / 100);
    AppendDigit(out, (value / 10) % 10);
    AppendDigit(out, value % 10);
}

void AppendNumber4(TMessageBuffer* out, int value)
{
    AppendDigit(out, value / 1000);
    AppendDigit(out, (value / 100) % 10);
    AppendDigit(out, (value / 10) % 10);
    AppendDigit(out, value % 10);
}

} // namespace

void FormatDateTime(TMessageBuffer* out, TInstant dateTime)
{
    tm localTime;
    dateTime.LocalTime(&localTime);
    AppendNumber4(out, localTime.tm_year + 1900);
    out->AppendChar('-');
    AppendNumber2(out, localTime.tm_mon + 1);
    out->AppendChar('-');
    AppendNumber2(out, localTime.tm_mday);
    out->AppendChar(' ');
    AppendNumber2(out, localTime.tm_hour);
    out->AppendChar(':');
    AppendNumber2(out, localTime.tm_min);
    out->AppendChar(':');
    AppendNumber2(out, localTime.tm_sec);
    out->AppendChar(',');
    AppendNumber3(out, dateTime.MilliSecondsOfSecond());
}

void FormatLevel(TMessageBuffer* out, ELogLevel level)
{
    static char chars[] = "?TDIWEF?";
    out->AppendChar(chars[static_cast<int>(level)]);
}

void FormatMessage(TMessageBuffer* out, const Stroka& message)
{
    for (auto current = message.begin(); current != message.end(); ++current) {
        char ch = *current;
        if (ch == '\n') {
            out->AppendString("\\n");
        } else if (ch == '\t') {
            out->AppendString("\\t");
        } else {
            out->AppendChar(ch);
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
