#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// Forward declaration, avoid including format.h directly here.
template <class... TArgs>
void Format(TStringBuilder* builder, const char* format, const TArgs&... args);

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
        FlushedLength_ += (Current_ - Begin_);
        size = std::max(size, static_cast<size_t>(1024));
        size_t capacity = FlushedLength_ + size;
        Str_.ReserveAndResize(capacity);
        Begin_ = Current_ = &*Str_.begin() + FlushedLength_;
        End_ = Begin_ + size;
        return Current_;
    }

    size_t GetLength() const
    {
        return FlushedLength_ + (Current_ - Begin_);
    }

    void Advance(size_t size)
    {
        Current_ += size;
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
        FlushedLength_ += (Current_ - Begin_);
        Begin_ = Current_ = End_ = nullptr;
        Str_.resize(FlushedLength_);
        return std::move(Str_);
    }

private:
    Stroka Str_;
    size_t FlushedLength_ = 0;

    char* Begin_ = nullptr;
    char* Current_ = nullptr;
    char* End_ = nullptr;

};

////////////////////////////////////////////////////////////////////////////////

//! Formatters enable customizable way to turn an object into a string.
//! This default implementation calls |ToString|.
struct TDefaultFormatter
{
    template <class T>
    Stroka operator () (const T& obj) const
    {
        return ToString(obj);
    }
};

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
    const Stroka& delimiter = ", ")
{
    Stroka result;
    for (auto current = begin; current != end; ++current) {
        if (current != begin) {
            result.append(delimiter);
        }
        result.append(formatter(*current));
    }
    return result;
}

//! A handy shortcut with default formatter.
template <class TIterator>
Stroka JoinToString(
    const TIterator& begin,
    const TIterator& end,
    const Stroka& delimiter = ", ")
{
    return JoinToString(begin, end, TDefaultFormatter(), delimiter);
}

//! Joins a collection of given items into a string intermixing them with the delimiter.
/*!
 *  The function assume the presence of begin()- and end()-like methods in the collection.
 *  
 *  \param collection A collection containing the items to be joined.
 *  \param delimiter A delimiter to be inserted between items. By default equals ", ".
 *  \return The resulting combined string.
 */
template <class TCollection, class TFormatter>
Stroka JoinToString(
    const TCollection& items,
    const TFormatter& formatter,
    const Stroka& delimiter = ", ")
{
    return JoinToString(items.begin(), items.end(), formatter, delimiter);
}

//! A handy shortcut with default formatter.
template <class TCollection>
Stroka JoinToString(
    const TCollection& items,
    const Stroka& delimiter = ", ")
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
    for (auto it = begin; it != end; ++it) {
        result.push_back(formatter(*it));
        if (result.size() == maxSize) {
            break;
        }
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
    return ConvertToStrings(collection.begin(), collection.end(), formatter, maxSize);
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

bool ParseBool(const Stroka& value);
Stroka FormatBool(bool value);

////////////////////////////////////////////////////////////////////////////////

Stroka DecodeEnumValue(const Stroka& value);
Stroka EncodeEnumValue(const Stroka& value);

template <class T>
inline T ParseEnum(
    const Stroka& value,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T&, TEnumBase<T>&>, int>::TType = 0)
{
    return T::FromString(DecodeEnumValue(value));
}

template <class T>
Stroka FormatEnum(
    T value,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T&, TEnumBase<T>&>, int>::TType = 0)
{
    return EncodeEnumValue(ToString(value));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
