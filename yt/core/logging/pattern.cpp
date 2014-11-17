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

// And here comes the old slow stuff.
using ::ToString;

Stroka ToString(const TMessageBuffer& out)
{
    return Stroka(out.GetData(), out.GetBytesWritten());
}

void SetupFormatter(TPatternFormatter* formatter, const TLogEvent& event)
{
    std::unique_ptr<TMessageBuffer> out(new TMessageBuffer());

    out->Reset();
    FormatLevel(out.get(), event.Level);
    formatter->AddProperty("level", ToString(*out));

    out->Reset();
    FormatDateTime(out.get(), event.DateTime);
    formatter->AddProperty("datetime", ToString(*out));

    out->Reset();
    FormatMessage(out.get(), event.Message);
    formatter->AddProperty("message", ToString(*out));

    formatter->AddProperty("category", event.Category);

    formatter->AddProperty("tab", "\t");

    if (event.FileName) {
        formatter->AddProperty("file", NFS::GetFileName(event.FileName));
    }

    if (event.Line != TLogEvent::InvalidLine) {
        formatter->AddProperty("line", ToString(event.Line));
    }

    if (event.ThreadId != NConcurrency::InvalidThreadId) {
        formatter->AddProperty("thread", ToString(event.ThreadId));
    }

    if (event.FiberId != NConcurrency::InvalidFiberId) {
        formatter->AddProperty("fiber", ToString(event.FiberId));
    }

    if (event.Function) {
        formatter->AddProperty("function", event.Function);
    }
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

Stroka FormatEvent(const TLogEvent& event, const Stroka& pattern)
{
    TPatternFormatter formatter;
    SetupFormatter(&formatter, event);
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
