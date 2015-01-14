#ifndef FORMAT_INL_H_
#error "Direct inclusion of this file is not allowed, include format.h"
#endif
#undef FORMAT_INL_H_

#include "mpl.h"
#include "string.h"
#include "nullable.h"
#include "enum.h"
#include "guid.h"
#include "assert.h"

#include <cctype>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

const char GenericSpecSymbol = 'v';

template <class TValue>
void FormatValue(TStringBuilder* builder, const TValue& value, const TStringBuf& format);

#ifdef __GNUC__
// Catch attempts to format 128-bit numbers early.
void FormatValue(TStringBuilder* builder, __int128 value, const TStringBuf& format) YDEPRECATED;
#endif

// TStringBuf
inline void FormatValue(TStringBuilder* builder, const TStringBuf& value, const TStringBuf& format)
{
    // Parse alignment.
    bool alignLeft = false;
    const char* current = format.begin();
    if (*current == '-') {
        alignLeft = true;
        ++current;
    }
    
    bool hasAlign = false;
    int alignSize = 0;
    while (*current >= '0' && *current <= '9') {
        hasAlign = true;
        alignSize = 10 * alignSize + (*current - '0');
        if (alignSize > 1000000) {
            builder->AppendString(STRINGBUF("<alignment overflow>"));
            return;
        }
        ++current;
    }

    int padding = 0;
    bool padLeft = false;
    bool padRight = false;
    if (hasAlign) {
        padding = alignSize - value.size();
        if (padding < 0) {
            padding = 0;
        }
        padLeft = !alignLeft;
        padRight = alignLeft;
    }

    bool singleQuotes = false;
    bool doubleQuotes = false;
    while (current < format.end()) {
        if (*current == 'q') {
            singleQuotes = true;
        } else if (*current == 'Q') {
            doubleQuotes = true;
        }
        ++current;
    }

    if (padLeft) {
        builder->AppendChar(' ', padding);
    }

    if (singleQuotes || doubleQuotes) {
        char int2hex = [] (unsigned char x) {
            YASSERT(x < 16);
            return x < 10 ? ('0' + x) : ('A' + x - 10);
        };
        for (const char* current = value.begin(); current < value.end(); ++current) {
            if (!std::isprint(*current) && !std::isspace(*current)) {
                builder->AppendString("\\x");
                builder->AppendChar(int2hex(static_cast<unsigned char>(*current) >> 4));
                builder->AppendChar(int2hex(static_cast<unsigned char>(*current) & 0xF));
            } else if ((singleQuotes && *current == '\'') || (doubleQuotes && *current == '\"')) {
                builder->AppendChar('\\');
                builder->AppendChar(*current);
            } else {
                builder->AppendChar(*current);
            }
        }
    } else {
        builder->AppendString(value);
    }

    if (padRight) {
        builder->AppendChar(' ', padding);
    }
}

// Stroka
inline void FormatValue(TStringBuilder* builder, const Stroka& value, const TStringBuf& format)
{
    FormatValue(builder, TStringBuf(value), format);
}

// const char*
inline void FormatValue(TStringBuilder* builder, const char* value, const TStringBuf& format)
{
    FormatValue(builder, TStringBuf(value), format);
}

// char
inline void FormatValue(TStringBuilder* builder, char value, const TStringBuf& format)
{
    FormatValue(builder, TStringBuf(&value, 1), format);
}

// bool
inline void FormatValue(TStringBuilder* builder, bool value, const TStringBuf& format)
{
    // Parse custom flags.
    bool lowercase = false;
    const char* current = format.begin();
    while (current != format.end()) {
        if (*current == 'l') {
            ++current;
            lowercase = true;
        } else if (*current == 'q' || *current == 'Q') {
            ++current;
        } else
            break;
    }

    auto str = lowercase
        ? (value ? STRINGBUF("true") : STRINGBUF("false"))
        : (value ? STRINGBUF("True") : STRINGBUF("False"));

    builder->AppendString(str);
}

// Default (via ToString).
template <class TValue, class = void>
struct TValueFormatter
{
    static void Do(TStringBuilder* builder, const TValue& value, const TStringBuf& format)
    {
        using ::ToString;
        FormatValue(builder, ToString(value), format);
    }
};

// Enums
template <class TEnum>
struct TValueFormatter<TEnum, typename std::enable_if<TEnumTraits<TEnum>::IsEnum>::type>
{
    static void Do(TStringBuilder* builder, TEnum value, const TStringBuf& format)
    {
        // Obtain literal.
        auto* literal = TEnumTraits<TEnum>::FindLiteralByValue(value);
        if (!literal) {
            builder->AppendFormat("%v(%v)",
                TEnumTraits<TEnum>::GetTypeName(),
                static_cast<typename TEnumTraits<TEnum>::TUnderlying>(value));
            return;
        }

        // Parse custom flags.
        bool lowercase = false;
        const char* current = format.begin();
        while (current != format.end()) {
            if (*current == 'l') {
                ++current;
                lowercase = true;
            } else if (*current == 'q' || *current == 'Q') {
                ++current;
            } else
                break;
        }

        if (lowercase) {
            CamelCaseToUnderscoreCase(builder, *literal);
        } else {
            builder->AppendString(*literal);
        }
    }
};

// Pointers
template <class T>
void FormatValue(TStringBuilder* builder, T* value, const TStringBuf& format)
{
    FormatValueStd(builder, value, format, STRINGBUF("p"));
}

// TGuid (specialize for performance reasons)
inline void FormatValue(TStringBuilder* builder, const TGuid& value, const TStringBuf& /*format*/)
{
    char* buf = builder->Preallocate(4 + 4 * 8);
    int count = sprintf(buf, "%x-%x-%x-%x",
        value.Parts32[3],
        value.Parts32[2],
        value.Parts32[1],
        value.Parts32[0]);
    builder->Advance(count);
}

// TNullable
inline void FormatValue(TStringBuilder* builder, TNull, const TStringBuf& /*format*/)
{
    builder->AppendString("<null>");
}

template <class T>
struct TValueFormatter<TNullable<T>>
{
    static void Do(TStringBuilder* builder, const TNullable<T>& value, const TStringBuf& format)
    {
        if (value) {
            FormatValue(builder, *value, format);
        } else {
            FormatValue(builder, Null, format);
        }
    }
};

template <class TValue>
void FormatValue(TStringBuilder* builder, const TValue& value, const TStringBuf& format)
{
    TValueFormatter<TValue>::Do(builder, value, format);
}

template <class TValue>
void FormatValueStd(TStringBuilder* builder, TValue value, const TStringBuf& format, const TStringBuf& genericSpec)
{
    const int MaxFormatSize = 64;
    const int MaxResultSize = 64;

    char formatBuf[MaxFormatSize];
    YCHECK(format.length() >= 1 && format.length() <= MaxFormatSize - 2); // one for %, one for \0
    formatBuf[0] = '%';
    if (format[format.length() - 1] == GenericSpecSymbol) {
        memcpy(formatBuf + 1, format.begin(), format.length() - 1);
        memcpy(formatBuf + format.length(), genericSpec.begin(), genericSpec.length());
        formatBuf[format.length() + genericSpec.length()] = '\0';
    } else {
        memcpy(formatBuf + 1, format.begin(), format.length());
        formatBuf[format.length() + 1] = '\0';
    }

    char* result = builder->Preallocate(MaxResultSize);
    size_t resultSize = snprintf(result, MaxResultSize, formatBuf, value);
    builder->Advance(resultSize);
}

#define IMPLEMENT_FORMAT_VALUE_STD(valueType, castType, genericSpec) \
    inline void FormatValue(TStringBuilder* builder, valueType value, const TStringBuf& format) \
    { \
        FormatValueStd(builder, static_cast<castType>(value), format, genericSpec); \
    }

IMPLEMENT_FORMAT_VALUE_STD(i8,              int,                STRINGBUF("d"))
IMPLEMENT_FORMAT_VALUE_STD(ui8,             unsigned int,       STRINGBUF("u"))
IMPLEMENT_FORMAT_VALUE_STD(i16,             int,                STRINGBUF("d"))
IMPLEMENT_FORMAT_VALUE_STD(ui16,            unsigned int,       STRINGBUF("u"))
IMPLEMENT_FORMAT_VALUE_STD(i32,             int,                STRINGBUF("d"))
IMPLEMENT_FORMAT_VALUE_STD(ui32,            unsigned int,       STRINGBUF("u"))
IMPLEMENT_FORMAT_VALUE_STD(long,            long,               STRINGBUF("ld"))
IMPLEMENT_FORMAT_VALUE_STD(unsigned long,   unsigned long,      STRINGBUF("lu"))
#ifdef _win_
IMPLEMENT_FORMAT_VALUE_STD(i64,             i64,                STRINGBUF(PRId64))
IMPLEMENT_FORMAT_VALUE_STD(ui64,            ui64,               STRINGBUF(PRIu64))
#endif
IMPLEMENT_FORMAT_VALUE_STD(double,          double,             STRINGBUF("lf"))
IMPLEMENT_FORMAT_VALUE_STD(float,           float,              STRINGBUF("f"))

#undef IMPLEMENT_STD_FORMAT_VALUE

////////////////////////////////////////////////////////////////////////////////

template <class TArgFormatter>
void FormatImpl(
    TStringBuilder* builder,
    const char* format,
    const TArgFormatter& argFormatter)
{
    size_t argIndex = 0;
    const char* current = format;
    while (true) {
        // Scan verbatim part until stop symbol.
        const char* verbatimBegin = current;
        const char* verbatimEnd = verbatimBegin;
        while (*verbatimEnd != '\0' && *verbatimEnd != '%') {
            ++verbatimEnd;
        }

        // Copy verbatim part, if any.
        size_t verbatimSize = verbatimEnd - verbatimBegin;
        if (verbatimSize > 0) {
            builder->AppendString(TStringBuf(verbatimBegin, verbatimSize));
        }

        // Handle stop symbol.
        current = verbatimEnd;
        if (*current == '\0')
            break;

        YASSERT(*current == '%');
        ++current;

        if (*current == '%') {
            // Verbatim %.
            builder->AppendChar('%');
            ++current;
        } else {
            // Scan format part until stop symbol.
            const char* argFormatBegin = current;
            const char* argFormatEnd = argFormatBegin;
            bool singleQuotes = false;
            bool doubleQuotes = false;

            while (
                *argFormatEnd != '\0' &&                  // end of format string
                *argFormatEnd != GenericSpecSymbol &&     // value in generic format
                *argFormatEnd != 'd' &&                   // others are standard specifiers supported by printf
                *argFormatEnd != 'i' &&
                *argFormatEnd != 'u' &&
                *argFormatEnd != 'o' &&
                *argFormatEnd != 'x' &&
                *argFormatEnd != 'X' &&
                *argFormatEnd != 'f' &&
                *argFormatEnd != 'F' &&
                *argFormatEnd != 'e' &&
                *argFormatEnd != 'E' &&
                *argFormatEnd != 'g' &&
                *argFormatEnd != 'G' &&
                *argFormatEnd != 'a' &&
                *argFormatEnd != 'A' &&
                *argFormatEnd != 'c' &&
                *argFormatEnd != 's' &&
                *argFormatEnd != 'p' &&
                *argFormatEnd != 'n')
            {
                if (*argFormatEnd == 'q') {
                    singleQuotes = true;
                } else if (*argFormatEnd == 'Q') {
                    doubleQuotes = true;
                }
                ++argFormatEnd;
            }

            // Handle end of format string.
            if (*argFormatEnd != '\0') {
                ++argFormatEnd;
            }

            // 'n' means 'nothing'; skip the argument.
            if (*argFormatBegin != 'n') {
                // Format argument.
                TStringBuf argFormat(argFormatBegin, argFormatEnd);
                if (singleQuotes) {
                    builder->AppendChar('\'');
                }
                if (doubleQuotes) {
                    builder->AppendChar('"');
                }
                argFormatter(argIndex++, builder, argFormat);
                if (singleQuotes) {
                    builder->AppendChar('\'');
                }
                if (doubleQuotes) {
                    builder->AppendChar('"');
                }
            }


            current = argFormatEnd;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

template <size_t IndexBase, class... TArgs>
struct TArgFormatterImpl;

template <size_t IndexBase>
struct TArgFormatterImpl<IndexBase>
{
    void operator() (size_t index, TStringBuilder* builder, const TStringBuf& /*format*/) const
    {
        builder->AppendString(STRINGBUF("<missing argument>"));
    }
};

template <size_t IndexBase, class THeadArg, class... TTailArgs>
struct TArgFormatterImpl<IndexBase, THeadArg, TTailArgs...>
{
    explicit TArgFormatterImpl(const THeadArg& headArg, const TTailArgs&... tailArgs)
        : HeadArg(headArg)
        , TailFormatter(tailArgs...)
    { }

    const THeadArg& HeadArg;
    TArgFormatterImpl<IndexBase + 1, TTailArgs...> TailFormatter;

    void operator() (size_t index, TStringBuilder* builder, const TStringBuf& format) const
    {
        YASSERT(index >= IndexBase);
        if (index == IndexBase) {
            FormatValue(builder, HeadArg, format);
        } else {
            TailFormatter(index, builder, format);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class... TArgs>
void Format(
    TStringBuilder* builder,
    const char* format,
    const TArgs&... args)
{
    TArgFormatterImpl<0, TArgs...> argFormatter(args...);
    FormatImpl(builder, format, argFormatter);
}

template <class... TArgs>
Stroka Format(
    const char* format,
    const TArgs&... args)
{
    TStringBuilder builder;
    Format(&builder, format, args...);
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
