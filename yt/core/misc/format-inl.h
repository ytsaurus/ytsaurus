#pragma once
#ifndef FORMAT_INL_H_
#error "Direct inclusion of this file is not allowed, include format.h"
#endif

#include "mpl.h"
#include "string.h"
#include "nullable.h"
#include "enum.h"
#include "guid.h"
#include "assert.h"
#include "range.h"

#include <cctype>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static const char GenericSpecSymbol = 'v';
static const char Int2Hex[] = "0123456789abcdef";

inline bool IsQuotationSpecSymbol(char symbol)
{
    return symbol == 'Q' || symbol == 'q';
}

template <class TValue>
void FormatValue(TStringBuilder* builder, const TValue& value, TStringBuf format);

#ifdef __GNUC__
// Catch attempts to format 128-bit numbers early.
#ifdef YDEPRECATED
void FormatValue(TStringBuilder* builder, __int128 value, TStringBuf format) YDEPRECATED();
#else
void FormatValue(TStringBuilder* builder, __int128 value, TStringBuf format) Y_DEPRECATED();
#endif
#endif

// TStringBuf
inline void FormatValue(TStringBuilder* builder, TStringBuf value, TStringBuf format)
{
    if (!format) {
        builder->AppendString(value);
        return;
    }

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
            builder->AppendString(AsStringBuf("<alignment overflow>"));
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
        for (const char* valueCurrent = value.begin(); valueCurrent < value.end(); ++valueCurrent) {
            char ch = *valueCurrent;
            if (!std::isprint(ch) && !std::isspace(ch)) {
                builder->AppendString("\\x");
                builder->AppendChar(Int2Hex[static_cast<ui8>(ch) >> 4]);
                builder->AppendChar(Int2Hex[static_cast<ui8>(ch) & 0xf]);
            } else if ((singleQuotes && ch == '\'') || (doubleQuotes && ch == '\"')) {
                builder->AppendChar('\\');
                builder->AppendChar(ch);
            } else {
                builder->AppendChar(ch);
            }
        }
    } else {
        builder->AppendString(value);
    }

    if (padRight) {
        builder->AppendChar(' ', padding);
    }
}

// TString
inline void FormatValue(TStringBuilder* builder, const TString& value, TStringBuf format)
{
    FormatValue(builder, TStringBuf(value), format);
}

// const char*
inline void FormatValue(TStringBuilder* builder, const char* value, TStringBuf format)
{
    FormatValue(builder, TStringBuf(value), format);
}

// char
inline void FormatValue(TStringBuilder* builder, char value, TStringBuf format)
{
    FormatValue(builder, TStringBuf(&value, 1), format);
}

// bool
inline void FormatValue(TStringBuilder* builder, bool value, TStringBuf format)
{
    // Parse custom flags.
    bool lowercase = false;
    const char* current = format.begin();
    while (current != format.end()) {
        if (*current == 'l') {
            ++current;
            lowercase = true;
        } else if (IsQuotationSpecSymbol(*current)) {
            ++current;
        } else
            break;
    }

    auto str = lowercase
        ? (value ? AsStringBuf("true") : AsStringBuf("false"))
        : (value ? AsStringBuf("True") : AsStringBuf("False"));

    builder->AppendString(str);
}

// Default (via ToString).
template <class TValue, class = void>
struct TValueFormatter
{
    static void Do(TStringBuilder* builder, const TValue& value, TStringBuf format)
    {
        using ::ToString;
        FormatValue(builder, ToString(value), format);
    }
};

// Enums
template <class TEnum>
struct TValueFormatter<TEnum, typename std::enable_if<TEnumTraits<TEnum>::IsEnum>::type>
{
    static void Do(TStringBuilder* builder, TEnum value, TStringBuf format)
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
            } else if (IsQuotationSpecSymbol(*current)) {
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

template <class TRange, class TFormatter>
TFormattableRange<TRange, TFormatter> MakeFormattableRange(
    const TRange& range,
    const TFormatter& formatter)
{
    return TFormattableRange<TRange, TFormatter>{range, formatter};
}

template <class TRange, class TFormatter>
void FormatRange(TStringBuilder* builder, const TRange& range, const TFormatter& formatter)
{
    builder->AppendChar('[');
    bool firstItem = true;
    for (const auto& item : range) {
        if (!firstItem) {
            builder->AppendString(DefaultJoinToStringDelimiter);
        }
        formatter(builder, item);
        firstItem = false;
    }
    builder->AppendChar(']');
}

// TFormattableRange
template <class TRange, class TFormatter>
struct TValueFormatter<TFormattableRange<TRange, TFormatter>>
{
    static void Do(TStringBuilder* builder, const TFormattableRange<TRange, TFormatter>& range, TStringBuf /*format*/)
    {
        FormatRange(builder, range.Range, range.Formatter);
    }
};

// TRange
template <class T>
struct TValueFormatter<TRange<T>>
{
    static void Do(TStringBuilder* builder, TRange<T> range, TStringBuf /*format*/)
    {
        FormatRange(builder, range, TDefaultFormatter());
    }
};

// TSharedRange
template <class T>
struct TValueFormatter<TSharedRange<T>>
{
    static void Do(TStringBuilder* builder, const TSharedRange<T>& range, TStringBuf /*format*/)
    {
        FormatRange(builder, range, TDefaultFormatter());
    }
};

// std::vector
template <class T>
struct TValueFormatter<std::vector<T>>
{
    static void Do(TStringBuilder* builder, const std::vector<T>& collection, TStringBuf /*format*/)
    {
        FormatRange(builder, collection, TDefaultFormatter());
    }
};

// SmallVector
template <class T, unsigned N>
struct TValueFormatter<SmallVector<T, N>>
{
    static void Do(TStringBuilder* builder, const SmallVector<T, N>& collection, TStringBuf /*format*/)
    {
        FormatRange(builder, collection, TDefaultFormatter());
    }
};

// std::set
template <class T>
struct TValueFormatter<std::set<T>>
{
    static void Do(TStringBuilder* builder, const std::set<T>& collection, TStringBuf /*format*/)
    {
        FormatRange(builder, collection, TDefaultFormatter());
    }
};

// THashSet
template <class T>
struct TValueFormatter<THashSet<T>>
{
    static void Do(TStringBuilder* builder, const THashSet<T>& collection, TStringBuf /*format*/)
    {
        FormatRange(builder, collection, TDefaultFormatter());
    }
};

// RepeatedField
template <class T>
struct TValueFormatter<::google::protobuf::RepeatedField<T>>
{
    static void Do(TStringBuilder* builder, const ::google::protobuf::RepeatedField<T>& collection, TStringBuf /*format*/)
    {
        FormatRange(builder, collection, TDefaultFormatter());
    }
};

// RepeatedPtrField
template <class T>
struct TValueFormatter<::google::protobuf::RepeatedPtrField<T>>
{
    static void Do(TStringBuilder* builder, const ::google::protobuf::RepeatedPtrField<T>& collection, TStringBuf /*format*/)
    {
        FormatRange(builder, collection, TDefaultFormatter());
    }
};

// TEnumIndexedVector
template <class T, class E>
struct TValueFormatter<TEnumIndexedVector<T, E>>
{
    static void Do(TStringBuilder* builder, const TEnumIndexedVector<T, E>& collection, TStringBuf format)
    {
        builder->AppendChar('{');
        bool firstItem = true;
        for (const auto& index : TEnumTraits<E>::GetDomainValues()) {
            if (!firstItem) {
                builder->AppendString(DefaultJoinToStringDelimiter);
            }
            FormatValue(builder, index, format);
            builder->AppendString(": ");
            FormatValue(builder, collection[index], format);
            firstItem = false;
        }
        builder->AppendChar('}');
    }
};

// TGuid
inline void FormatValue(TStringBuilder* builder, TGuid value, TStringBuf /*format*/)
{
    char* begin = builder->Preallocate(8 * 4 + 3);
    char* end = WriteGuidToBuffer(begin, value);
    builder->Advance(end - begin);
}

// TNullable
inline void FormatValue(TStringBuilder* builder, TNull, TStringBuf /*format*/)
{
    builder->AppendString(AsStringBuf("<null>"));
}

template <class T>
struct TValueFormatter<TNullable<T>>
{
    static void Do(TStringBuilder* builder, const TNullable<T>& value, TStringBuf format)
    {
        if (value) {
            FormatValue(builder, *value, format);
        } else {
            FormatValue(builder, Null, format);
        }
    }
};

template <class TValue>
void FormatValue(TStringBuilder* builder, const TValue& value, TStringBuf format)
{
    TValueFormatter<TValue>::Do(builder, value, format);
}

template <class TValue>
void FormatValueViaSprintf(
    TStringBuilder* builder,
    TValue value,
    TStringBuf format,
    TStringBuf genericSpec)
{
    const int MaxFormatSize = 64;
    const int SmallResultSize = 64;

    auto copyFormat = [] (char* destination, const char* source, int length) {
        int position = 0;
        for (int index = 0; index < length; ++index) {
            if (IsQuotationSpecSymbol(source[index])) {
                continue;
            }
            destination[position] = source[index];
            ++position;
        }
        return destination + position;
    };

    char formatBuf[MaxFormatSize];
    YCHECK(format.length() >= 1 && format.length() <= MaxFormatSize - 2); // one for %, one for \0
    formatBuf[0] = '%';
    if (format[format.length() - 1] == GenericSpecSymbol) {
        char* formatEnd = copyFormat(formatBuf + 1, format.begin(), format.length() - 1);
        memcpy(formatEnd, genericSpec.begin(), genericSpec.length());
        formatEnd[genericSpec.length()] = '\0';
    } else {
        char* formatEnd = copyFormat(formatBuf + 1, format.begin(), format.length());
        *formatEnd = '\0';
    }

    char* result = builder->Preallocate(SmallResultSize);
    size_t resultSize = snprintf(result, SmallResultSize, formatBuf, value);
    if (resultSize >= SmallResultSize) {
        result = builder->Preallocate(resultSize + 1);
        YCHECK(snprintf(result, resultSize + 1, formatBuf, value) == static_cast<int>(resultSize));
    }
    builder->Advance(resultSize);
}

template <class TValue>
char* WriteIntToBufferBackwards(char* buffer, TValue value);

template <class TValue>
void FormatValueViaHelper(TStringBuilder* builder, TValue value, TStringBuf format, TStringBuf genericSpec)
{
    if (format == AsStringBuf("v")) {
        const int MaxResultSize = 64;
        char buffer[MaxResultSize];
        char* end = buffer + MaxResultSize;
        char* start = WriteIntToBufferBackwards(end, value);
        builder->AppendString(TStringBuf(start, end));
    } else {
        FormatValueViaSprintf(builder, value, format, genericSpec);
    }
}

#define XX(valueType, castType, genericSpec) \
    inline void FormatValue(TStringBuilder* builder, valueType value, TStringBuf format) \
    { \
        FormatValueViaHelper(builder, static_cast<castType>(value), format, genericSpec); \
    }

XX(i8,              int,                AsStringBuf("d"))
XX(ui8,             unsigned int,       AsStringBuf("u"))
XX(i16,             int,                AsStringBuf("d"))
XX(ui16,            unsigned int,       AsStringBuf("u"))
XX(i32,             int,                AsStringBuf("d"))
XX(ui32,            unsigned int,       AsStringBuf("u"))
XX(long,            long,               AsStringBuf("ld"))
XX(unsigned long,   unsigned long,      AsStringBuf("lu"))

#undef XX

#define XX(valueType, castType, genericSpec) \
    inline void FormatValue(TStringBuilder* builder, valueType value, TStringBuf format) \
    { \
        FormatValueViaSprintf(builder, static_cast<castType>(value), format, genericSpec); \
    }

XX(double,          double,             AsStringBuf("lf"))
XX(float,           float,              AsStringBuf("f"))

#undef XX

// Pointers
template <class T>
void FormatValue(TStringBuilder* builder, T* value, TStringBuf format)
{
    FormatValueViaSprintf(builder, value, format, AsStringBuf("p"));
}

// TDuration (specialize for performance reasons)
inline void FormatValue(TStringBuilder* builder, const TDuration& value, TStringBuf format)
{
    FormatValue(builder, value.MilliSeconds(), format);
}

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

        Y_ASSERT(*current == '%');
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
    void operator() (size_t /*index*/, TStringBuilder* builder, TStringBuf /*format*/) const
    {
        builder->AppendString(AsStringBuf("<missing argument>"));
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

    void operator() (size_t index, TStringBuilder* builder, TStringBuf format) const
    {
        Y_ASSERT(index >= IndexBase);
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
TString Format(
    const char* format,
    const TArgs&... args)
{
    TStringBuilder builder;
    Format(&builder, format, args...);
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
