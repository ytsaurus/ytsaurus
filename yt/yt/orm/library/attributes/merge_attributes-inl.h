#ifndef MERGE_ATTRIBUTES_INL_H_
#error "Direct inclusion of this file is not allowed, include merge_attributes.h"
// For the sake of sane code completion.
#include "merge_attributes.h"
#endif

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <algorithm>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

template <std::ranges::range TRange, class TPathProj, class TIsEtcProj>
void ValidateSortedPaths(const TRange& paths, TPathProj pathProj, TIsEtcProj etcProj)
{
    auto begin = paths.begin();

    THROW_ERROR_EXCEPTION_IF(
        begin != paths.end() && std::invoke(pathProj, *begin).empty() && !std::invoke(etcProj, *begin),
        "Merging on empty path is supported for etc schemas only");

    while ((begin = std::ranges::adjacent_find(
        begin,
        paths.end(),
        [] (const NYPath::TYPath& lhs, const NYPath::TYPath& rhs)
        {
            return NYPath::HasPrefix(rhs, lhs);
        },
        pathProj)) != paths.end())
    {
        THROW_ERROR_EXCEPTION_UNLESS(std::invoke(etcProj, *begin),
            "Paths sorted for merge cannot contain intersections except for etc");
        ++begin;
    }
}

template <typename TType, std::invocable<TType> TPathProj, std::predicate<TType> TForceKeep>
void SortAndRemoveNestedPaths(std::vector<TType>& collection, TPathProj pathProj, TForceKeep forceKeepProj)
{
    if (collection.empty()) {
        return;
    }

    std::ranges::sort(collection.begin(), collection.end(), /*comp*/ {}, pathProj);

    int lastRemainingPath = 0;
    int lastPath = 0;
    for (int i = 1; i < std::ssize(collection); ++i) {
        if (!NYPath::HasPrefix(std::invoke(pathProj, collection[i]), std::invoke(pathProj, collection[lastRemainingPath]))) {
            collection[++lastPath] = collection[i];
            lastRemainingPath = lastPath;
        } else if (std::invoke(forceKeepProj, collection[i]) && collection[lastPath] != collection[i]) {
            collection[++lastPath] = collection[i];
        }
    }

    collection.resize(lastPath + 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
