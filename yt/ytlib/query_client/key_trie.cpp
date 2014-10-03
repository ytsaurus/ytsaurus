#include "stdafx.h"
#include "key_trie.h"

namespace NYT {
namespace NQueryClient {

using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

TKeyTrieNode ReduceKeyTrie(const TKeyTrieNode& keyTrie)
{
    // TODO(lukyan): If keyTrie is too big, reduce its size
    return keyTrie;
}

int CompareBound(const TBound& lhs, const TBound& rhs, bool lhsDir, bool rhsDir)
{
    auto rank = [] (bool direction, bool included) {
        // <  - (false, fasle)
        // >  - (true, false)
        // <= - (false, true)
        // >= - (true, true)

        // (< x) < (>= x) < (<= x) < (> x)
        return (included? -1 : 2) * (direction? 1 : -1);
    };

    int result = CompareRowValues(lhs.Value, rhs.Value);
    return result == 0
        ? rank(lhsDir, lhs.Included) - rank(rhsDir, rhs.Included)
        : result;
};

template <class TEachCallback>
void MergeBounds(
    const std::vector<TBound>& lhs,
    const std::vector<TBound>& rhs,
    TEachCallback eachCallback)
{
    auto first = lhs.begin();
    auto second = rhs.begin();

    bool firstIsOpen = true;
    bool secondIsOpen = true;

    while (first != lhs.end() && second != rhs.end()) {
        if (CompareBound(*first, *second, firstIsOpen, secondIsOpen) < 0) {
            eachCallback(*first, firstIsOpen);
            ++first;
            firstIsOpen = !firstIsOpen;
        } else {
            eachCallback(*second, secondIsOpen);
            ++second;
            secondIsOpen = !secondIsOpen;
        }
    }

    while (first != lhs.end()) {
        eachCallback(*first, firstIsOpen);
        ++first;
        firstIsOpen = !firstIsOpen;
    }

    while (second != rhs.end()) {
        eachCallback(*second, secondIsOpen);
        ++second;
        secondIsOpen = !secondIsOpen;
    }
}

std::vector<TBound> UniteBounds(
    const std::vector<TBound>& lhs,
    const std::vector<TBound>& rhs)
{
    int cover = 0;
    std::vector<TBound> result;
    bool resultIsOpen = false;

    MergeBounds(lhs, rhs, [&] (TBound bound, bool isOpen) {
        if (isOpen? ++cover == 1 : --cover == 0) {
            if (result.empty() || !(result.back() == bound && isOpen == resultIsOpen)) {
                result.push_back(bound);
                resultIsOpen = !resultIsOpen;
            }
        }
    });

    return result;
}

std::vector<TBound> IntersectBounds(
    const std::vector<TBound>& lhs,
    const std::vector<TBound>& rhs)
{
    int cover = 0;
    std::vector<TBound> result;
    bool resultIsOpen = false;

    MergeBounds(lhs, rhs, [&] (TBound bound, bool isOpen) {
        if (isOpen? ++cover == 2 : --cover == 1) {
            if (result.empty() || !(result.back() == bound && isOpen == resultIsOpen)) {
                result.push_back(bound);
                resultIsOpen = !resultIsOpen;
            }
        }
    });

    return result;
}

TKeyTrieNode UniteKeyTrie(const TKeyTrieNode& lhs, const TKeyTrieNode& rhs)
{
    if (lhs.Offset < rhs.Offset) {
        return rhs;
    } else if (lhs.Offset > rhs.Offset) {
        return lhs;
    }
    TKeyTrieNode result;
    result.Offset = lhs.Offset;

    std::vector<TBound> bounds = UniteBounds(lhs.Bounds, rhs.Bounds);

    std::vector<TBound> deletedPoints;

    deletedPoints.emplace_back(MakeUnversionedSentinelValue(EValueType::Min), true);

    auto addValue = [&] (const std::pair<TUnversionedValue, TKeyTrieNode>& next) {
        auto found = result.Next.find(next.first);

        if (found != result.Next.end()) {
            found->second = UniteKeyTrie(found->second, next.second);
        } else {
            result.Next.insert(next);

            deletedPoints.emplace_back(next.first, false);
            deletedPoints.emplace_back(next.first, false);
        }
    };

    for (const auto& next : lhs.Next) {
        addValue(next);
    }
        
    for (const auto& next : rhs.Next) {
        addValue(next);
    }

    deletedPoints.emplace_back(MakeUnversionedSentinelValue(EValueType::Max), true);

    result.Bounds = IntersectBounds(bounds, deletedPoints);

    return result;
};

TKeyTrieNode IntersectKeyTrie(const TKeyTrieNode& lhs, const TKeyTrieNode& rhs)
{
    if (lhs.Offset < rhs.Offset) {
        TKeyTrieNode result = lhs;
        for (auto& next : result.Next) {
            next.second = IntersectKeyTrie(next.second, rhs);
        }
        return result;
    } else if (lhs.Offset > rhs.Offset) {
        TKeyTrieNode result = rhs;
        for (auto& next : result.Next) {
            next.second = IntersectKeyTrie(next.second, lhs);
        }
        return result;
    }
    TKeyTrieNode result;
    result.Offset = lhs.Offset;
    result.Bounds = IntersectBounds(lhs.Bounds, rhs.Bounds);

    auto covers = [&] (const std::vector<TBound>& bounds, const TUnversionedValue& point) {
        auto found = std::lower_bound(
            bounds.begin(),
            bounds.end(),
            point,
            [] (const TBound& bound, const TUnversionedValue& point) {
                return bound.Value < point;
            });

        bool isClose = (found - bounds.begin()) & 1;
        if (found != bounds.end()) {
            return (found->Value != point) == isClose;
        } else {
            return false;
        }
    };

    for (const auto& next : lhs.Next) {
        if (covers(rhs.Bounds, next.first)) {
            result.Next.insert(next);
        }
    }

    for (const auto& next : rhs.Next) {
        if (covers(lhs.Bounds, next.first)) {
            result.Next.insert(next);
        }
    }

    for (const auto& next : lhs.Next) {
        auto found = rhs.Next.find(next.first);
        if (found != rhs.Next.end()) {
            result.Next[next.first] = IntersectKeyTrie(found->second, next.second);
        } 
    }
    return result;
}

void GetRangesFromTrieWithinRangeImpl(
    const TKeyRange& keyRange,
    TRowBuffer* rowBuffer,
    int keySize,
    std::vector<TKeyRange>* result,
    const TKeyTrieNode& trie,
    std::vector<TUnversionedValue> prefix = std::vector<TUnversionedValue>(),
    bool refineLower = true,
    bool refineUpper = true)
{
    size_t offset = prefix.size();
    
    if (refineLower && offset >= keyRange.first.GetCount()) {
        refineLower = false;
    }

    if (refineUpper && offset >= keyRange.second.GetCount()) {
        return;
    }

    YCHECK(offset <= keySize);
    YCHECK(!refineLower || offset < keyRange.first.GetCount());
    YCHECK(!refineUpper || offset < keyRange.second.GetCount());

    auto getFirstKeyRangeComponent = [&] (size_t index) {
        // EValueType::Min because lower bound from keyRange is included
        return index < keyRange.first.GetCount()
            ? keyRange.first[index]
            : MakeUnversionedSentinelValue(EValueType::Min);
    };

    auto getSecondKeyRangeComponent = [&] (size_t index) {
        // EValueType::Min because upper bound from keyRange is excluded
        return index < keyRange.second.GetCount()
            ? keyRange.second[index]
            : MakeUnversionedSentinelValue(EValueType::Min);
    };

    if (trie.Offset > offset) {
        if (refineLower && refineUpper && keyRange.first[offset] == keyRange.second[offset]) {
            prefix.emplace_back(keyRange.first[offset]);
            GetRangesFromTrieWithinRangeImpl(
                keyRange,
                rowBuffer,
                keySize,
                result,
                trie,
                prefix,
                true,
                true);
        } else {
            TKeyRange range;
            TUnversionedOwningRowBuilder builder(prefix.size());

            for (size_t i = 0; i < prefix.size(); ++i) {
                builder.AddValue(prefix[i]);
            }
            for (size_t i = prefix.size(); i < keySize; ++i) {
                // EValueType::Min because lower bound from prefix is included
                builder.AddValue(refineLower
                    ? getFirstKeyRangeComponent(i)
                    : MakeUnversionedSentinelValue(EValueType::Min));
            }
            range.first = builder.FinishRow();

            for (size_t i = 0; i < prefix.size(); ++i) {
                // We need to make result range with excluded upper bound
                // it could also be done outside this function
                builder.AddValue(i + 1 != keySize
                    ? prefix[i]
                    : GetValueSuccessor(prefix[i], rowBuffer));
            }
            for (size_t i = prefix.size(); i < std::max(keySize, keyRange.second.GetCount()); ++i) {
                // EValueType::Max because upper bound from prefix is included 
                builder.AddValue(refineUpper
                    ? getSecondKeyRangeComponent(i)
                    : MakeUnversionedSentinelValue(EValueType::Max));
            }
            range.second = builder.FinishRow();
            result->push_back(range);
        }
        return;
    }

    YCHECK(trie.Offset == offset);

    auto resultBounds = trie.Bounds;
    YCHECK(!(resultBounds.size() & 1));

    for (size_t i = 0; i + 1 < resultBounds.size(); i += 2) {

        auto lower = resultBounds[i];
        auto upper = resultBounds[i + 1];

        YCHECK(lower.Value < upper.Value);

        auto keyRangeLowerBound = TBound(keyRange.first[offset], true);
        auto keyRangeUpperBound = TBound(keyRange.second[offset], offset + 1 != keyRange.second.GetCount());

        bool lowerBoundRefined = false;
        if (refineLower) {
            if (CompareBound(upper, keyRangeLowerBound, false, true) < 0) {
                continue;
            } else if (CompareBound(lower, keyRangeLowerBound, true, true) <= 0) {
                lowerBoundRefined = true;
            }
        }

        bool upperBoundRefined = false;
        if (refineUpper) {
            if (CompareBound(lower, keyRangeUpperBound, true, false) > 0) {
                continue;
            } else if (CompareBound(upper, keyRangeUpperBound, false, false) >= 0) {
                upperBoundRefined = true;
            }
        }

        TKeyRange range;
        TUnversionedOwningRowBuilder builder(prefix.size());

        for (size_t j = 0; j < prefix.size(); ++j) {
            builder.AddValue(prefix[j]);
        }

        if (lowerBoundRefined) {
            for (size_t j = offset; j < keySize; ++j) {
                builder.AddValue(getFirstKeyRangeComponent(j));
            }
        } else {
            size_t rangeSize = keySize;

            builder.AddValue(
                offset + 1 == rangeSize && !lower.Included
                    ? GetValueSuccessor(lower.Value, rowBuffer)
                    : lower.Value);

            for (size_t j = offset + 1; j < rangeSize; ++j) {
                builder.AddValue(MakeUnversionedSentinelValue(
                    lower.Included
                        ? EValueType::Min
                        : EValueType::Max));
            }
        }
        
        range.first = builder.FinishRow();

        for (size_t j = 0; j < prefix.size(); ++j) {
            builder.AddValue(prefix[j]);
        }

        if (upperBoundRefined) {
            for (size_t j = offset; j < std::max(keySize, keyRange.second.GetCount()); ++j) {
                builder.AddValue(getSecondKeyRangeComponent(j));
            }
        } else {
            size_t rangeSize = std::max(keySize, keyRange.second.GetCount());

            builder.AddValue(
                offset + 1 == rangeSize && upper.Included
                    ? GetValueSuccessor(upper.Value, rowBuffer)
                    : upper.Value);

            for (size_t j = offset + 1; j < rangeSize; ++j) {
                builder.AddValue(MakeUnversionedSentinelValue(
                    upper.Included
                        ? EValueType::Max
                        : EValueType::Min));
            }
        }

        range.second = builder.FinishRow();
        result->push_back(range);
    }

    prefix.emplace_back();

    for (const auto& next : trie.Next) {
        auto value = next.first;

        bool refineLowerNext = false;
        if (refineLower) {
            if (value < keyRange.first[offset]) {
                continue;
            } else if (value == keyRange.first[offset]) {
                refineLowerNext = true;
            }
        }

        bool refineUpperNext = false;
        if (refineUpper) {
            if (value > keyRange.second[offset]) {
                continue;
            } else if (value == keyRange.second[offset]) {
                refineUpperNext = true;
            }
        }

        prefix.back() = value;

        GetRangesFromTrieWithinRangeImpl(
            keyRange,
            rowBuffer,
            keySize,
            result,
            next.second,
            prefix,
            refineLowerNext,
            refineUpperNext);
    }
}

std::vector<TKeyRange> GetRangesFromTrieWithinRange(
    const TKeyRange& keyRange,
    TRowBuffer* rowBuffer,
    int keySize,
    const TKeyTrieNode& trie)
{
    std::vector<TKeyRange> result;

    GetRangesFromTrieWithinRangeImpl(keyRange, rowBuffer, keySize, &result, trie);
    return result;
}
////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
