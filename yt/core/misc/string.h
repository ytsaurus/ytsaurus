#pragma once

#include "public.h"

#include "string_builder.h"

#include <util/string/strip.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Formatters enable customizable way to turn an object into a string.
//! This default implementation uses |FormatValue|.
struct TDefaultFormatter
{
    template <class T>
    void operator()(TStringBuilderBase* builder, const T& obj) const
    {
        FormatValue(builder, obj, AsStringBuf("v"));
    }
};

static constexpr TStringBuf DefaultJoinToStringDelimiter = AsStringBuf(", ");
static constexpr TStringBuf DefaultKeyValueDelimiter = AsStringBuf(": ");
static constexpr TStringBuf DefaultRangeEllipsisFormat = AsStringBuf("...");

//! Joins a range of items into a string intermixing them with the delimiter.
/*!
 *  \param builder String builder where the output goes.
 *  \param begin Iterator pointing to the first item (inclusive).
 *  \param end Iterator pointing to the last item (not inclusive).
 *  \param formatter Formatter to apply to the items.
 *  \param delimiter A delimiter to be inserted between items: ", " by default.
 *  \return The resulting combined string.
 */
template <class TIterator, class TFormatter>
void JoinToString(
    TStringBuilderBase* builder,
    const TIterator& begin,
    const TIterator& end,
    const TFormatter& formatter,
    TStringBuf delimiter = DefaultJoinToStringDelimiter)
{
    for (auto current = begin; current != end; ++current) {
        if (current != begin) {
            builder->AppendString(delimiter);
        }
        formatter(builder, *current);
    }
}

template <class TIterator, class TFormatter>
TString JoinToString(
    const TIterator& begin,
    const TIterator& end,
    const TFormatter& formatter,
    TStringBuf delimiter = DefaultJoinToStringDelimiter)
{
    TStringBuilder builder;
    JoinToString(&builder, begin, end, formatter, delimiter);
    return builder.Flush();
}

//! A handy shortcut with default formatter.
template <class TIterator>
TString JoinToString(
    const TIterator& begin,
    const TIterator& end,
    TStringBuf delimiter = DefaultJoinToStringDelimiter)
{
    return JoinToString(begin, end, TDefaultFormatter(), delimiter);
}

//! Joins a collection of given items into a string intermixing them with the delimiter.
/*!
 *  \param collection A collection containing the items to be joined.
 *  \param formatter Formatter to apply to the items.
 *  \param delimiter A delimiter to be inserted between items; ", " by default.
 */
template <class TCollection, class TFormatter>
TString JoinToString(
    const TCollection& collection,
    const TFormatter& formatter,
    TStringBuf delimiter = DefaultJoinToStringDelimiter)
{
    using std::begin;
    using std::end;
    return JoinToString(begin(collection), end(collection), formatter, delimiter);
}

//! A handy shortcut with the default formatter.
template <class TCollection>
TString JoinToString(
    const TCollection& collection,
    TStringBuf delimiter = DefaultJoinToStringDelimiter)
{
    return JoinToString(collection, TDefaultFormatter(), delimiter);
}

//! Concatenates a bunch of TStringBuf-like instances into TString.
template <class... Ts>
TString ConcatToString(Ts... args)
{
    size_t length = 0;
    ((length += args.length()), ...);

    TString result;
    result.reserve(length);
    (result.append(args), ...);

    return result;
}

//! Converts a range of items into strings.
template <class TIter, class TFormatter>
std::vector<TString> ConvertToStrings(
    const TIter& begin,
    const TIter& end,
    const TFormatter& formatter,
    size_t maxSize = std::numeric_limits<size_t>::max())
{
    std::vector<TString> result;
    for (auto it = begin; it != end; ++it) {
        TStringBuilder builder;
        formatter(&builder, *it);
        result.push_back(builder.Flush());
        if (result.size() == maxSize) {
            break;
        }
    }
    return result;
}

//! A handy shortcut with the default formatter.
template <class TIter>
std::vector<TString> ConvertToStrings(
    const TIter& begin,
    const TIter& end,
    size_t maxSize = std::numeric_limits<size_t>::max())
{
    return ConvertToStrings(begin, end, TDefaultFormatter(), maxSize);
}

//! Converts a given collection of items into strings.
/*!
 *  \param collection A collection containing the items to be converted.
 *  \param formatter Formatter to apply to the items.
 *  \param maxSize Size limit for the resulting vector.
 */
template <class TCollection, class TFormatter>
std::vector<TString> ConvertToStrings(
    const TCollection& collection,
    const TFormatter& formatter,
    size_t maxSize = std::numeric_limits<size_t>::max())
{
    using std::begin;
    using std::end;
    return ConvertToStrings(begin(collection), end(collection), formatter, maxSize);
}

//! A handy shortcut with default formatter.
template <class TCollection>
std::vector<TString> ConvertToStrings(
    const TCollection& collection,
    size_t maxSize = std::numeric_limits<size_t>::max())
{
    return ConvertToStrings(collection, TDefaultFormatter(), maxSize);
}

////////////////////////////////////////////////////////////////////////////////

void UnderscoreCaseToCamelCase(TStringBuilderBase* builder, TStringBuf str);
TString UnderscoreCaseToCamelCase(TStringBuf str);

void CamelCaseToUnderscoreCase(TStringBuilderBase* builder, TStringBuf str);
TString CamelCaseToUnderscoreCase(TStringBuf str);

TString TrimLeadingWhitespaces(const TString& str);
TString Trim(const TString& str, const TString& whitespaces);

bool TryParseBool(const TString& value, bool& result);
bool ParseBool(const TString& value);
TStringBuf FormatBool(bool value);

////////////////////////////////////////////////////////////////////////////////

//! Implemented for |[u]i(32|64)|.
template <class T>
char* WriteIntToBufferBackwards(char* ptr, T value);

char* WriteGuidToBuffer(char* ptr, TGuid value);

////////////////////////////////////////////////////////////////////////////////

TString DecodeEnumValue(TStringBuf value);
TString EncodeEnumValue(TStringBuf value);

template <class T>
T ParseEnum(TStringBuf value)
{
    static_assert(TEnumTraits<T>::IsEnum);

    if constexpr (TEnumTraits<T>::IsBitEnum) {
        T result = {};
        TStringBuf token;
        while (value.TrySplit('|', token, value)) {
            result |= TEnumTraits<T>::FromString(DecodeEnumValue(StripString(token)));
        }
        result |= TEnumTraits<T>::FromString(DecodeEnumValue(StripString(value)));
        return result;
    } else {
        return TEnumTraits<T>::FromString(DecodeEnumValue(value));
    }
}

void FormatUnknownEnum(TStringBuilderBase* builder, TStringBuf name, i64 value);

template <class T>
void FormatEnum(TStringBuilderBase* builder, T value, bool lowerCase)
{
    static_assert(TEnumTraits<T>::IsEnum);

    auto formatScalarValue = [builder, lowerCase] (T value) {
        auto* literal = TEnumTraits<T>::FindLiteralByValue(value);
        if (!literal) {
            YT_VERIFY(!TEnumTraits<T>::IsBitEnum);
            FormatUnknownEnum(
                builder,
                TEnumTraits<T>::GetTypeName(),
                static_cast<typename TEnumTraits<T>::TUnderlying>(value));
            return;
        }

        if (lowerCase) {
            CamelCaseToUnderscoreCase(builder, *literal);
        } else {
            builder->AppendString(*literal);
        }
    };

    if constexpr (TEnumTraits<T>::IsBitEnum) {
        auto* literal = TEnumTraits<T>::FindLiteralByValue(value);
        if (literal) {
            formatScalarValue(value);
            return;
        }
        auto first = true;
        for (auto scalarValue : TEnumTraits<T>::GetDomainValues()) {
            if (Any(value & scalarValue)) {
                if (!first) {
                    builder->AppendString(AsStringBuf(" | "));
                }
                first = false;
                formatScalarValue(scalarValue);
            }
        }
    } else {
        formatScalarValue(value);
    }
}

template <class T>
TString FormatEnum(T value, typename TEnumTraits<T>::TType* = nullptr)
{
    TStringBuilder b;
    FormatEnum(&b, value, /* lowerCase */ true);
    return b.Flush();
}

////////////////////////////////////////////////////////////////////////////////

struct TCaseInsensitiveStringHasher
{
    size_t operator()(TStringBuf arg) const;
};

struct TCaseInsensitiveStringEqualityComparer
{
    bool operator()(TStringBuf lhs, TStringBuf rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
