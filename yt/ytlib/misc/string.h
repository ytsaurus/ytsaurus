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
    using ::ToString;
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
    for (TIter it = begin; it != end; ++it) {
        result.push_back(it->ToString());
        if (result.size() == maxSize) {
            break;
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

inline Stroka UnderscoreCaseToCamelCase(const Stroka& data)
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

inline Stroka CamelCaseToUnderscoreCase(const Stroka& data)
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

inline Stroka TrimLeadingWhitespace(const Stroka& data)
{
    for (int i = 0; i < data.size(); ++i) {
        if (data[i] != ' ') {
            return data.substr(i);
        }
    }
    return "";
}

////////////////////////////////////////////////////////////////////////////////

inline bool ParseBool(const Stroka& value)
{
    if (value == "true") {
        return true;
    } else if (value == "false") {
        return false;
    } else {
        ythrow yexception() << Sprintf("Could not parse boolean value %s",
            ~Stroka(value).Quote());
    }
}

inline Stroka FormatBool(bool value)
{
    return value ? "true" : "false";
}

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
