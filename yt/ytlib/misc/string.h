#pragma once

#include "common.h"
#include "foreach.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Joins a range of items into a string intermixing them with the delimiter.
/*!
 *  The function calls the global #::ToString for conversion.
 *  \param begin Iterator pointing to the first item (inclusive).
 *  \param end Iterator pointing to the last item (not inclusive).
 *  \param delimiter A delimiter to be inserted between items. By default equals ", ".
 *  \return The resulting combined string.
 */
template <class TIterator>
Stroka JoinToString(const TIterator& begin, const TIterator& end, Stroka delimiter = ", ")
{
    Stroka result;
    for (auto current = begin; current != end; ++current) {
        if (current != begin) {
            result.append(delimiter);
        }
        result.append(ToString(*current));
    }
    return result;
}

//! Joins a collection of given items into a string intermixing them with the delimiter.
/*!
 *  The function assume the presence of begin()- and end()-like methods in the collection
 *  and calls the global #::ToString for conversion. 
 *  \param collection A collection containing the items to be joined.
 *  \param delimiter A delimiter to be inserted between items. By default equals ", ".
 *  \return The resulting combined string.
 */
template <class TCollection>
Stroka JoinToString(const TCollection& items, Stroka delimiter = ", ")
{
    return JoinToString(items.begin(), items.end(), delimiter);
}

template <class TIter>
std::vector<Stroka> ConvertToStrings(TIter begin, TIter end, size_t maxSize)
{
    std::vector<Stroka> result;
    for (auto it = begin; it != end; ++it) {
        result.push_back(it->ToString());
        if (result.size() == maxSize) {
            break;
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

Stroka UnderscoreCaseToCamelCase(const Stroka& data);
Stroka CamelCaseToUnderscoreCase(const Stroka& data);

Stroka TrimLeadingWhitespace(const Stroka& data);

bool ParseBool(const Stroka& value);
Stroka FormatBool(bool value);

////////////////////////////////////////////////////////////////////////////////

template <class T>
inline T ParseEnum(
    const Stroka& value,
    typename NMpl::TEnableIf<NMpl::TIsConvertible< T, TEnumBase<T> >, int>::TType = 0)
{
    return T::FromString(UnderscoreCaseToCamelCase(value));
}


template <class T>
Stroka FormatEnum(
    T value,
    typename NMpl::TEnableIf<NMpl::TIsConvertible< T, TEnumBase<T> >, int>::TType = 0)
{
    return CamelCaseToUnderscoreCase(value.ToString());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
