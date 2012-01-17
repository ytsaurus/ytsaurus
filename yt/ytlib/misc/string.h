#pragma once

#include "common.h"

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
    for (TIterator current = begin; current != end; ++current) {
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

////////////////////////////////////////////////////////////////////////////////

template <class TIter>
yvector<Stroka> ConvertToStrings(TIter begin, size_t maxSize)
{
    yvector<Stroka> result;
    result.reserve(maxSize);
    for (TIter it = begin; result.size() < maxSize; ++it) {
        result.push_back(it->ToString());
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

inline Stroka ConvertUnderscoreCaseToCamelCase(const Stroka& data)
{
    Stroka result;
    bool upper = true;
    FOREACH (char c, data) {
        if (c == '_') {
            upper = true;
        } else {
            if (upper) {
                c = std::toupper(c);
            }
            result.push_back(c);
            upper = false;
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

inline Stroka ConvertCamelCaseToUnderscoreCase(const Stroka& data)
{
    Stroka result;
    bool first = true;
    FOREACH (char c, data) {
        if (std::isupper(c)) {
            if (!first) {
                result.push_back('_');
            }
            result.push_back(std::tolower(c));
        } else {
            result.push_back(c);
        }
        first = false;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

inline bool ParseBool(const Stroka& value)
{
    if (value == "true") {
        return true;
    } else if (value == "false") {
        return false;
    } else {
        ythrow yexception()
            << Sprintf("Could not parse boolean parameter (Value: %s)", ~value);
    }
}

////////////////////////////////////////////////////////////////////////////////

inline Stroka FormatBool(bool value)
{
    return value ? "true" : "false";
}

////////////////////////////////////////////////////////////////////////////////

template <class T, typename NYT::NMpl::TEnableIf<
    NYT::NMpl::TIsConvertible< T, TEnumBase<T> >, int
    >::TType = 0>
inline T ParseEnum(const Stroka& value)
{
    return T::FromString(ConvertUnderscoreCaseToCamelCase(value));
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
Stroka FormatEnum(
    T value,
    typename NYT::NDetail::TEnableIfConvertible<T, TEnumBase<T> >::TType = 
        NYT::NDetail::TEmpty())
{
    return ConvertCamelCaseToUnderscoreCase(value.ToString());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
