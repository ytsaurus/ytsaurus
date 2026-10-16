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

template <std::ranges::input_range TRange, class TPathProj, class TIsEtcProj>
void ValidateSortedPaths(const TRange& paths, TPathProj pathProj, TIsEtcProj etcProj)
{
    auto begin = paths.begin();

    THROW_ERROR_EXCEPTION_IF(
        begin != paths.end() && std::invoke(pathProj, *begin).empty() && !std::invoke(etcProj, *begin),
        "Merging on empty path is supported for etc schemas only");

    while ((begin = std::ranges::adjacent_find(
        begin,
        paths.end(),
        [] (const NYPath::TYPath& lhs, const NYPath::TYPath& rhs) {
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

    std::ranges::sort(collection, std::less{}, pathProj);

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

template <std::ranges::input_range TRange, class TPathProj, class TIsEtcProj>
TMergeAttributesPlan::TMergeAttributesPlan(
    const TRange& paths,
    TPathProj pathProj,
    TIsEtcProj isEtcProj)
{
    ValidateSortedPaths(paths, pathProj, isEtcProj);

    TCompactVector<std::string, 4> pathToCurrentMap;
    for (const auto& item : paths) {
        bool isEtc = std::invoke(isEtcProj, item);

        NYPath::TTokenizer tokenizer(std::invoke(pathProj, item));
        tokenizer.Expect(NYPath::ETokenType::StartOfStream);
        tokenizer.Advance();

        TCompactVector<std::string, 4> literals;
        while (tokenizer.GetType() != NYPath::ETokenType::EndOfStream) {
            tokenizer.Skip(NYPath::ETokenType::Slash);
            tokenizer.Expect(NYPath::ETokenType::Literal);
            literals.push_back(tokenizer.GetLiteralValue());
            tokenizer.Advance();
        }

        int matchedPrefixLength = 0;
        while (matchedPrefixLength < std::ssize(pathToCurrentMap) &&
            matchedPrefixLength < std::ssize(literals) &&
            pathToCurrentMap[matchedPrefixLength] == literals[matchedPrefixLength])
        {
            ++matchedPrefixLength;
        }

        if (matchedPrefixLength == std::ssize(literals)) {
            YT_VERIFY(isEtc);
        }

        auto& transition = Transitions_.emplace_back(TTransition{
            .MapCountToClose = static_cast<int>(std::ssize(pathToCurrentMap) - matchedPrefixLength),
            .IsEtc = isEtc,
        });

        pathToCurrentMap.resize(matchedPrefixLength);
        for (int index = matchedPrefixLength; index < std::ssize(literals); ++index) {
            if (index + 1 < std::ssize(literals) || isEtc) {
                pathToCurrentMap.push_back(literals[index]);
            }
        }

        transition.Literals.assign(
            std::make_move_iterator(literals.begin()) + matchedPrefixLength,
            std::make_move_iterator(literals.end()));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
