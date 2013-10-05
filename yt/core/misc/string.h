#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Formatters enable customizable way to turn an object into a string.
//! This default implementation calls |ToString|.
struct TDefaultFormatter
{
    template <class T>
    Stroka Format(const T& obj) const
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
        result.append(formatter.Format(*current));
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
        result.push_back(formatter.Format(*it));
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

Stroka UnderscoreCaseToCamelCase(const Stroka& data);
Stroka CamelCaseToUnderscoreCase(const Stroka& data);

Stroka TrimLeadingWhitespaces(const Stroka& data);

bool ParseBool(const Stroka& value);
Stroka FormatBool(bool value);

////////////////////////////////////////////////////////////////////////////////

template <class T>
inline T ParseEnum(
    const Stroka& value,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T&, TEnumBase<T>&>, int>::TType = 0)
{
    return T::FromString(UnderscoreCaseToCamelCase(value));
}


template <class T>
Stroka FormatEnum(
    T value,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T&, TEnumBase<T>&>, int>::TType = 0)
{
    return CamelCaseToUnderscoreCase(value.ToString());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
