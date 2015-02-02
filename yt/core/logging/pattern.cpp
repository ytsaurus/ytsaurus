#include "stdafx.h"
#include "pattern.h"

#include <core/misc/fs.h>
#include <core/misc/common.h>

#ifdef YT_USE_SSE42
#include <emmintrin.h>
#include <pmmintrin.h>
#endif

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
    auto current = message.begin();

    auto FormatChar = [&] () {
        char ch = *current;
        if (ch == '\n') {
            out->AppendString("\\n");
        } else if (ch == '\t') {
            out->AppendString("\\t");
        } else {
            out->AppendChar(ch);
        }
        ++current;
    };

#ifdef YT_USE_SSE42
    auto vectorN = _mm_set1_epi8('\n');
    auto vectorT = _mm_set1_epi8('\t');
#endif

    while (current < message.end()) {
#ifdef YT_USE_SSE42
        // Use SSE for optimization.

        if (current + 16 >= message.end() || out->GetBytesRemaining() < 16) {
            FormatChar();
        } else {
            const void* inPtr = &(*current);
            void* outPtr = out->GetCursor();
            auto value = _mm_lddqu_si128(static_cast<const __m128i*>(inPtr));
            if (_mm_movemask_epi8(_mm_cmpeq_epi8(value, vectorN)) ||
                _mm_movemask_epi8(_mm_cmpeq_epi8(value, vectorT))) {
                for (int index = 0; index < 16; ++index) {
                    FormatChar();
                }
            } else {
                _mm_storeu_si128(static_cast<__m128i*>(outPtr), value);
                out->Advance(16);
                current += 16;
            }
        }
#else
        // Unoptimized version.
        FormatChar();
#endif
    }
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
