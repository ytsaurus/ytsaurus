#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// Forward declaration, avoid including format.h directly here.
template <class... TArgs>
void Format(
    TStringBuilder* builder,
    const char* format,
    const TArgs&... args);

//! A simple helper for constructing strings by a sequence of appends.
class TStringBuilder
    : private TNonCopyable
{
public:
    char* Preallocate(size_t size)
    {
        // Fast path.
        if (Current_ + size <= End_) {
            return Current_;
        }

        // Slow path.
        size_t committedLength = GetLength();
        size = std::max(size, static_cast<size_t>(1024));
        Str_.ReserveAndResize(committedLength + size);
        Begin_ = Current_ = &*Str_.begin() + committedLength;
        End_ = Begin_ + size;
        return Current_;
    }

    size_t GetLength() const
    {
        return Current_ ? Current_ - Str_.Data() : 0;
    }

    TStringBuf GetBuffer() const
    {
        return TStringBuf(&*Str_.begin(), GetLength());
    }

    void Advance(size_t size)
    {
        Current_ += size;
        YASSERT(Current_ <= End_);
    }

    void AppendChar(char ch)
    {
        *Preallocate(1) = ch;
        Advance(1);
    }

    void AppendChar(char ch, int n)
    {
        YASSERT(n >= 0);
        char* dst = Preallocate(n);
        memset(dst, ch, n);
        Advance(n);
    }

    void AppendString(const TStringBuf& str)
    {
        char* dst = Preallocate(str.length());
        memcpy(dst, str.begin(), str.length());
        Advance(str.length());
    }

    void AppendString(const char* str)
    {
        AppendString(TStringBuf(str));
    }

    template <class... TArgs>
    void AppendFormat(const char* format, const TArgs&... args)
    {
        Format(this, format, args...);
    }

    Stroka Flush()
    {
        Str_.resize(GetLength());
        Begin_ = Current_ = End_ = nullptr;
        return std::move(Str_);
    }

private:
    Stroka Str_;

    char* Begin_ = nullptr;
    char* Current_ = nullptr;
    char* End_ = nullptr;

};

inline void FormatValue(
    TStringBuilder* builder,
    const TStringBuilder& value,
    const TStringBuf& /*format*/)
{
    builder->AppendString(value.GetBuffer());
}

////////////////////////////////////////////////////////////////////////////////

//! Formatters enable customizable way to turn an object into a string.
//! This default implementation calls |FormatValue|.
struct TDefaultFormatter
{
    template <class T>
    void operator () (TStringBuilder* builder, const T& obj) const
    {
        FormatValue(builder, obj, "%v");
    }
};

template <class TFormatter, class TValue>
struct TFormatted
{
    TFormatter Formatter;
    TValue Value;
};

template <class TFormatter, class TValue>
TFormatted<TFormatter, TValue> MakeFormatted(TFormatter&& formatter, TValue&& value)
{
    return TFormatted<TFormatter, TValue>{
        std::forward<TFormatter>(formatter),
        std::forward<TValue>(value)};
}

template <class TFormatter, class TValue>
void FormatValue(
    TStringBuilder* builder,
    const TFormatted<TFormatter, TValue>& formatted,
    const TStringBuf& /*format*/)
{
    formatted.Formatter(builder, formatted.Value);
}

////////////////////////////////////////////////////////////////////////////////

//! Joins a range of items into a string intermixing them with the delimiter.
/*!
 *  The function calls the global #::ToString for conversion.
 *  \param begin Iterator pointing to the first item (inclusive).
 *  \param end Iterator pointing to the last item (not inclusive).
 *  \param delimiter A delimiter to be inserted between items. By default equals ", ".
 *  \return The resulting combined string.
 */
template <class TIterator, class TFormatter>
Stroka JoinToString(
    const TIterator& begin,
    const TIterator& end,
    const TFormatter& formatter,
    const char* delimiter = ", ")
{
    TStringBuilder builder;
    for (auto current = begin; current != end; ++current) {
        if (current != begin) {
            builder.AppendString(delimiter);
        }
        formatter(&builder, *current);
    }
    return builder.Flush();
}

//! A handy shortcut with default formatter.
template <class TIterator>
Stroka JoinToString(
    const TIterator& begin,
    const TIterator& end,
    const char* delimiter = ", ")
{
    return JoinToString(begin, end, TDefaultFormatter(), delimiter);
}

//! Joins a collection of given items into a string intermixing them with the delimiter.
/*!
 *  \param collection A collection containing the items to be joined.
 *  \param delimiter A delimiter to be inserted between items. By default equals ", ".
 *  \return The resulting combined string.
 */
template <class TCollection, class TFormatter>
Stroka JoinToString(
    const TCollection& items,
    const TFormatter& formatter,
    const char* delimiter = ", ")
{
    using std::begin;
    using std::end;
    return JoinToString(begin(items), end(items), formatter, delimiter);
}

//! A handy shortcut with default formatter.
template <class TCollection>
Stroka JoinToString(
    const TCollection& items,
    const char* delimiter = ", ")
{
    return JoinToString(items, TDefaultFormatter(), delimiter);
}

//! Converts a range of items into strings.
template <class TIter, class TFormatter>
std::vector<Stroka> ConvertToStrings(
    const TIter& begin,
    const TIter& end,
    const TFormatter& formatter,
    size_t maxSize = std::numeric_limits<size_t>::max())
{
    std::vector<Stroka> result;
    for (auto it = begin; it != end && result.size() < maxSize; ++it) {
        TStringBuilder builder;
        formatter(&builder, *it);
        result.push_back(builder.Flush());
    }
    return result;
}

//! A handy shortcut with default formatter.
template <class TIter>
std::vector<Stroka> ConvertToStrings(
    const TIter& begin,
    const TIter& end,
    size_t maxSize = std::numeric_limits<size_t>::max())
{
    return ConvertToStrings(begin, end, TDefaultFormatter(), maxSize);
}

//! Converts a collection of given items into strings.
template <class TCollection, class TFormatter>
std::vector<Stroka> ConvertToStrings(
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
std::vector<Stroka> ConvertToStrings(
    const TCollection& collection,
    size_t maxSize = std::numeric_limits<size_t>::max())
{
    return ConvertToStrings(collection, TDefaultFormatter(), maxSize);
}

////////////////////////////////////////////////////////////////////////////////

void UnderscoreCaseToCamelCase(TStringBuilder* builder, const TStringBuf& str);
Stroka UnderscoreCaseToCamelCase(const TStringBuf& str);

void CamelCaseToUnderscoreCase(TStringBuilder* builder, const TStringBuf& str);
Stroka CamelCaseToUnderscoreCase(const TStringBuf& str);

Stroka TrimLeadingWhitespaces(const Stroka& str);
Stroka Trim(const Stroka& str, const Stroka& whitespaces);

bool ParseBool(const Stroka& value);
TStringBuf FormatBool(bool value);

////////////////////////////////////////////////////////////////////////////////

Stroka DecodeEnumValue(const Stroka& value);
Stroka EncodeEnumValue(const Stroka& value);

template <class T>
inline T ParseEnum(const Stroka& value, typename TEnumTraits<T>::TType* = 0)
{
    return TEnumTraits<T>::FromString(DecodeEnumValue(value));
}

template <class T>
Stroka FormatEnum(T value, typename TEnumTraits<T>::TType* = 0)
{
    return EncodeEnumValue(ToString(value));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
