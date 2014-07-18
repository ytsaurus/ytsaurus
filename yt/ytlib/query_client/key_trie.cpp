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

std::vector<TUnversionedValue> MergeBounds(
    std::vector<std::pair<TUnversionedValue, bool>>& bounds,
    int overlayCount = 0)
{
    std::sort(bounds.begin(), bounds.end());

    int cover = 0;
    std::vector<TUnversionedValue> result;

    for (const auto& bound : bounds) {
        if (bound.second) {
            --cover;
        } else {
            ++cover;
        }

        bool currentIsActive = result.size() & 1;
        if (currentIsActive != cover > overlayCount) {
            if (!result.empty() && result.back() == bound.first) {
                result.pop_back();
            } else {
                result.emplace_back(bound.first);
            }
        }
    }

    return result;
}

TKeyTrieNode UniteKeyTrie(const TKeyTrieNode& lhs, const TKeyTrieNode& rhs, TRowBuffer* rowBuffer)
{
    if (lhs.Offset < rhs.Offset) {
        return rhs;
    } else if (lhs.Offset > rhs.Offset) {
        return lhs;
    }
    TKeyTrieNode result;
    result.Offset = lhs.Offset;

    std::vector<std::pair<TUnversionedValue, bool>> bounds;

    for (size_t i = 0; i < lhs.Bounds.size(); ++i) {
        bounds.emplace_back(lhs.Bounds[i], bool(i & 1));
    }

    for (size_t j = 0; j < rhs.Bounds.size(); ++j) {
        bounds.emplace_back(rhs.Bounds[j], bool(j & 1));
    }

    for (const auto& next : lhs.Next) {
        bounds.emplace_back(next.first, true);
        bounds.emplace_back(GetValueSuccessor(next.first, rowBuffer), false);
    }

    for (const auto& next : rhs.Next) {
        bounds.emplace_back(next.first, true);
        bounds.emplace_back(GetValueSuccessor(next.first, rowBuffer), false);
    }

    result.Bounds = MergeBounds(bounds);

    auto addValue = [&] (const std::pair<TUnversionedValue, TKeyTrieNode>& next) {
        auto found = result.Next.find(next.first);

        if (found != result.Next.end()) {
            found->second = UniteKeyTrie(found->second, next.second, rowBuffer);
        } else {
            result.Next.insert(next);
        }
    };

    for (const auto& next : lhs.Next) {
        addValue(next);
    }
        
    for (const auto& next : rhs.Next) {
        addValue(next);
    }

    return result;
};

TKeyTrieNode IntersectKeyTrie(const TKeyTrieNode& lhs, const TKeyTrieNode& rhs, TRowBuffer* rowBuffer)
{
    if (lhs.Offset < rhs.Offset) {
        TKeyTrieNode result = lhs;
        for (auto& next : result.Next) {
            next.second = IntersectKeyTrie(next.second, rhs, rowBuffer);
        }
        return result;
    } else if (lhs.Offset > rhs.Offset) {
        TKeyTrieNode result = rhs;
        for (auto& next : result.Next) {
            next.second = IntersectKeyTrie(next.second, lhs, rowBuffer);
        }
        return result;
    }
    TKeyTrieNode result;
    result.Offset = lhs.Offset;
    std::vector<std::pair<TUnversionedValue, bool>> bounds;

    for (size_t i = 0; i < lhs.Bounds.size(); ++i) {
        bounds.emplace_back(lhs.Bounds[i], bool(i & 1));
    }

    for (size_t j = 0; j < rhs.Bounds.size(); ++j) {
        bounds.emplace_back(rhs.Bounds[j], bool(j & 1));
    }

    for (const auto& next : lhs.Next) {
        bounds.emplace_back(next.first, true);
        bounds.emplace_back(GetValueSuccessor(next.first, rowBuffer), false);
    }

    for (const auto& next : rhs.Next) {
        bounds.emplace_back(next.first, true);
        bounds.emplace_back(GetValueSuccessor(next.first, rowBuffer), false);
    }

    result.Bounds = MergeBounds(bounds, true);

    for (const auto& next : lhs.Next) {
        bool covers = !(std::upper_bound(
            rhs.Bounds.begin(),
            rhs.Bounds.end(),
            next.first) - rhs.Bounds.begin() & 1);
        if (covers) {
            result.Next.insert(next);
        }
    }

    for (const auto& next : rhs.Next) {
        bool covers = !(std::upper_bound(
            lhs.Bounds.begin(),
            lhs.Bounds.end(),
            next.first) - lhs.Bounds.begin() & 1);
        if (covers) {
            result.Next.insert(next);
        }
    }

    for (const auto& next : lhs.Next) {
        auto found = rhs.Next.find(next.first);
        if (found != rhs.Next.end()) {
            result.Next[next.first] = IntersectKeyTrie(found->second, next.second, rowBuffer);
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

    auto getFirstKeyRangeComponent = [&] (bool refine, size_t index) {
        return refine && index < keyRange.first.GetCount() ? 
            keyRange.first[index] : MakeUnversionedSentinelValue(EValueType::Min);
    };

    auto getSecondKeyRangeComponent = [&] (bool refine, size_t index) {
        return refine && index < keyRange.second.GetCount() ? 
            keyRange.second[index] : MakeUnversionedSentinelValue(EValueType::Min);
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
                builder.AddValue(getFirstKeyRangeComponent(refineLower, i));
            }
            range.first = builder.GetRowAndReset();

            for (size_t i = 0; i < prefix.size(); ++i) {
                builder.AddValue(i + 1 != keySize? prefix[i] : GetValueSuccessor(prefix[i], rowBuffer));
            }
            for (size_t i = prefix.size(); i < std::max(keySize, keyRange.second.GetCount()); ++i) {
                if (refineUpper) {
                    builder.AddValue(i < keyRange.second.GetCount() ? 
                        keyRange.second[i] : MakeUnversionedSentinelValue(EValueType::Min));
                } else {
                    builder.AddValue(MakeUnversionedSentinelValue(EValueType::Max));
                }
            }
            range.second = builder.GetRowAndReset();
            result->push_back(range);
        }
        return;
    }

    YCHECK(trie.Offset == offset);

    auto resultBounds = trie.Bounds;
    YCHECK(!(resultBounds.size() & 1));

    for (size_t i = 0; i + 1 < resultBounds.size(); i += 2) {

        YCHECK(resultBounds[i] < resultBounds[i + 1]);

        bool lowerBoundRefined = false;
        if (refineLower) {
            if (resultBounds[i + 1] <= keyRange.first[offset]) {
                continue;
            } else if (resultBounds[i] <= keyRange.first[offset]) {
                lowerBoundRefined = true;
            }
        }

        bool upperBoundRefined = false;
        if (refineUpper) {
            if (resultBounds[i] > keyRange.second[offset] 
                || offset + 1 == keyRange.second.GetCount() && resultBounds[i] == keyRange.second[offset]) {
                continue;
            } else if (resultBounds[i + 1] > keyRange.second[offset]) {
                upperBoundRefined = true;
            }
        }

        TKeyRange range;
        TUnversionedOwningRowBuilder builder(prefix.size());

        for (size_t j = 0; j < prefix.size(); ++j) {
            builder.AddValue(prefix[j]);
        }
        builder.AddValue(lowerBoundRefined? keyRange.first[offset] : resultBounds[i]);
        for (size_t j = prefix.size() + 1; j < keySize; ++j) {
            builder.AddValue(getFirstKeyRangeComponent(lowerBoundRefined, j));
        }
        range.first = builder.GetRowAndReset();

        for (size_t j = 0; j < prefix.size(); ++j) {
            builder.AddValue(prefix[j]);
        }
        builder.AddValue(upperBoundRefined? keyRange.second[offset] : resultBounds[i + 1]);
        for (size_t j = prefix.size() + 1; j < std::max(keySize, keyRange.second.GetCount()); ++j) {
            builder.AddValue(getSecondKeyRangeComponent(upperBoundRefined, j));
        }
        range.second = builder.GetRowAndReset();
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
