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
template<class TIterator>
Stroka JoinToString(const TIterator& begin, const TIterator& end, Stroka delimiter = ", ")
{
    Stroka result;
    for (TIterator current = begin; current != end; ++current) {
        if (current != begin) {
            result.append(delimiter);
        }
        result.append(::ToString(*current));
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
template<class TContainer>
Stroka JoinToString(const TContainer& items, Stroka delimiter = ", ")
{
    return JoinToString(items.begin(), items.end(), delimiter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
