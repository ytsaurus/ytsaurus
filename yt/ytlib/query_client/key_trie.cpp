#include "stdafx.h"
#include "key_trie.h"
#include "plan_helpers.h"

namespace NYT {
namespace NQueryClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TKeyTriePtr ReduceKeyTrie(TKeyTriePtr keyTrie)
{
    // TODO(lukyan): If keyTrie is too big, reduce its size
    return keyTrie;
}

struct TKeyTrieComparer
{
    bool operator () (const std::pair<TValue, TKeyTriePtr>& element, TValue pivot) const {
        return element.first < pivot;
    }
    bool operator () (TValue pivot, const std::pair<TValue, TKeyTriePtr>& element) const {
        return pivot < element.first;
    }
    bool operator () (const std::pair<TValue, TKeyTriePtr>& lhs, const std::pair<TValue, TKeyTriePtr>& rhs) const {
        return lhs.first < rhs.first;
    }
};

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

    MergeBounds(
        lhs,
        rhs,
        [&] (TBound bound, bool isOpen) {
            if (isOpen ? cover++ == 1 : --cover == 1) {
                if (result.empty() || !(result.back() == bound && isOpen == resultIsOpen)) {
                    result.push_back(bound);
                    resultIsOpen = !resultIsOpen;
                }
            }
        });

    return result;
}

TKeyTriePtr UniteKeyTrie(const std::vector<TKeyTriePtr>& tries)
{
    if (tries.empty()) {
        return TKeyTrie::Empty();
    } else if (tries.size() == 1) {
        return tries.front();
    }

    std::vector<TKeyTriePtr> maxTries;
    size_t offset = 0;
    for (const auto& trie : tries) {
        if (!trie) {
            return TKeyTrie::Universal();
        }

        if (trie->Offset > offset) {
            maxTries.clear();
            offset = trie->Offset;
        }

        if (trie->Offset == offset) {
            maxTries.push_back(trie);
        }
    }

    std::vector<std::pair<TValue, TKeyTriePtr>> groups;
    for (const auto& trie : maxTries) {
        for (auto& next : trie->Next) {
            groups.push_back(std::move(next));
        }
    }

    std::sort(groups.begin(), groups.end(), TKeyTrieComparer());

    auto result = New<TKeyTrie>(offset);
    std::vector<TKeyTriePtr> unique;

    auto it = groups.begin();
    auto end = groups.end();
    while (it != end) {
        unique.clear();
        auto same = it;
        for (; same != end && *same == *it; ++same) {
            unique.push_back(same->second);
        }
        result->Next.emplace_back(it->first, UniteKeyTrie(unique));
        it = same;
    }

    std::vector<std::vector<TBound>> bounds;
    for (const auto& trie : maxTries) {
        if (!trie->Bounds.empty()) {
            bounds.push_back(std::move(trie->Bounds));
        }
    }

    while (bounds.size() > 1) {
        size_t i = 0;
        while (2 * i + 1 < bounds.size()) {
            bounds[i] = UniteBounds(bounds[2 * i], bounds[2 * i + 1]);
            ++i;
        }
        bounds.resize(i);
    }

    YCHECK(bounds.size() <= 1);
    if (!bounds.empty()) {
        std::vector<TBound> deletedPoints;

        deletedPoints.emplace_back(MakeUnversionedSentinelValue(EValueType::Min), true);
        for (const auto& next : result->Next) {
            deletedPoints.emplace_back(next.first, false);
            deletedPoints.emplace_back(next.first, false);
        }
        deletedPoints.emplace_back(MakeUnversionedSentinelValue(EValueType::Max), true);

        result->Bounds = IntersectBounds(bounds.front(), deletedPoints);
    }

    return result;
}

TKeyTriePtr TKeyTrie::FromLowerBound(const TKey& bound)
{
    auto result = TKeyTrie::Universal();

    for (int offset = 0; offset < bound.GetCount(); ++offset) {
        if (bound[offset].Type != EValueType::Min && bound[offset].Type != EValueType::Max) {
            auto node = New<TKeyTrie>(offset);

            if (offset + 1 < bound.GetCount()) {
                if (bound[offset + 1].Type == EValueType::Min) {
                    node->Bounds.emplace_back(bound[offset], true);
                    node->Bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Max), true);
                } else if (bound[offset + 1].Type == EValueType::Max) {
                    node->Bounds.emplace_back(bound[offset], false);
                    node->Bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Max), true);
                } else {
                    node->Next.emplace_back(bound[offset], TKeyTrie::Universal());
                }
            } else {
                node->Bounds.emplace_back(bound[offset], true);
                node->Bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Max), true);
            }

            result = IntersectKeyTrie(result, node);
        }
    }

    return result;
}

TKeyTriePtr TKeyTrie::FromUpperBound(const TKey& bound)
{
    auto result = TKeyTrie::Universal();

    for (int offset = 0; offset < bound.GetCount(); ++offset) {
        if (bound[offset].Type != EValueType::Min && bound[offset].Type != EValueType::Max) {
            auto node = New<TKeyTrie>(offset);

            if (offset + 1 < bound.GetCount()) {
                if (bound[offset + 1].Type == EValueType::Min) {
                    node->Bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Min), true);
                    node->Bounds.emplace_back(bound[offset], false);
                } else if (bound[offset + 1].Type == EValueType::Max) {
                    node->Bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Min), true);
                    node->Bounds.emplace_back(bound[offset], true);
                } else {
                    node->Next.emplace_back(bound[offset], TKeyTrie::Universal());
                }
            } else {
                node->Bounds.emplace_back(MakeUnversionedSentinelValue(EValueType::Min), true);
                node->Bounds.emplace_back(bound[offset], false);
            }

            result = IntersectKeyTrie(result, node);
        }
    }

    return result;
}

TKeyTriePtr TKeyTrie::FromRange(const TKeyRange& range)
{
    return IntersectKeyTrie(FromLowerBound(range.first), FromUpperBound(range.second));
}

TKeyTriePtr UniteKeyTrie(TKeyTriePtr lhs, TKeyTriePtr rhs)
{
    return UniteKeyTrie({lhs, rhs});
};

TKeyTriePtr IntersectKeyTrie(TKeyTriePtr lhs, TKeyTriePtr rhs)
{
    auto lhsOffset = lhs ? lhs->Offset : std::numeric_limits<size_t>::max();
    auto rhsOffset = rhs ? rhs->Offset : std::numeric_limits<size_t>::max();

    if (lhsOffset < rhsOffset) {
        auto result = New<TKeyTrie>(*lhs);
        for (auto& next : result->Next) {
            next.second = IntersectKeyTrie(next.second, rhs);
        }
        return result;
    }

    if (lhsOffset > rhsOffset) {
        auto result = New<TKeyTrie>(*rhs);
        for (auto& next : result->Next) {
            next.second = IntersectKeyTrie(next.second, lhs);
        }
        return result;
    }

    if (!lhs && !rhs) {
        return nullptr;
    }

    YCHECK(lhs);
    YCHECK(rhs);

    auto result = New<TKeyTrie>(lhs->Offset);
    result->Bounds = IntersectBounds(lhs->Bounds, rhs->Bounds);

    // Iterate through resulting bounds and convert singleton ranges into
    // new edges in the trie. This enables futher range limiting.
    auto it = result->Bounds.begin();
    auto jt = result->Bounds.begin();
    auto kt = result->Bounds.end();
    while (it < kt) {
        const auto& lhs = *it++;
        const auto& rhs = *it++;
        if (lhs == rhs) {
            result->Next.emplace_back(lhs.Value, TKeyTrie::Universal());
        } else {
            if (std::distance(jt, it) > 2) {
                *jt++ = lhs;
                *jt++ = rhs;
            } else {
                ++jt; ++jt;
            }
        }
    }

    result->Bounds.erase(jt, kt);

    auto covers = [] (const std::vector<TBound>& bounds, const TValue& point) {
        auto found = std::lower_bound(
            bounds.begin(),
            bounds.end(),
            point,
            [] (const TBound& bound, const TValue& point) {
                return bound.Value < point;
            });

        bool isClose = (found - bounds.begin()) & 1;
        if (found != bounds.end()) {
            return (found->Value != point) == isClose;
        } else {
            return false;
        }
    };

    for (const auto& next : lhs->Next) {
        if (covers(rhs->Bounds, next.first)) {
            result->Next.push_back(next);
        }
    }

    for (const auto& next : rhs->Next) {
        if (covers(lhs->Bounds, next.first)) {
            result->Next.push_back(next);
        }
    }

    for (const auto& next : lhs->Next) {
        auto eq = std::equal_range(rhs->Next.begin(), rhs->Next.end(), next.first, TKeyTrieComparer());
        if (eq.first != eq.second) {
            result->Next.emplace_back(next.first, IntersectKeyTrie(eq.first->second, next.second));
        }
    }

    std::sort(result->Next.begin(), result->Next.end(), TKeyTrieComparer());
    return result;
}

void GetRangesFromTrieWithinRangeImpl(
    const TRowRange& keyRange,
    TKeyTriePtr trie,
    std::vector<std::pair<TRow, TRow>>* result,
    TRowBufferPtr rowBuffer,
    bool insertUndefined,
    std::vector<TValue> prefix = std::vector<TValue>(),
    bool refineLower = true,
    bool refineUpper = true)
{
    auto lowerBoundSize = keyRange.first.GetCount();
    auto upperBoundSize = keyRange.second.GetCount();

    size_t offset = prefix.size();
    
    if (refineLower && offset >= lowerBoundSize) {
        refineLower = false;
    }

    if (refineUpper && offset >= upperBoundSize) {
        return;
    }

    YCHECK(!refineLower || offset < lowerBoundSize);
    YCHECK(!refineUpper || offset < upperBoundSize);

    TUnversionedRowBuilder builder(offset);

    auto trieOffset = trie ? trie->Offset : std::numeric_limits<size_t>::max();

    auto makeValue = [] (TUnversionedValue value, int id) {
        value.Id = id;
        return value;
    };

    if (trieOffset > offset) {
        if (refineLower && refineUpper && keyRange.first[offset] == keyRange.second[offset]) {
            prefix.emplace_back(makeValue(keyRange.first[offset], offset));
            GetRangesFromTrieWithinRangeImpl(
                keyRange,
                trie,
                result,
                rowBuffer,
                insertUndefined,
                prefix,
                true,
                true);
        } else if (trie && insertUndefined) {
            prefix.emplace_back(MakeUnversionedSentinelValue(EValueType::TheBottom));
            GetRangesFromTrieWithinRangeImpl(
                keyRange,
                trie,
                result,
                rowBuffer,
                insertUndefined,
                prefix,
                false,
                false);
        } else {
            std::pair<TRow, TRow> range;
            for (size_t i = 0; i < offset; ++i) {
                builder.AddValue(prefix[i]);
            }

            if (refineLower) {
                for (size_t i = offset; i < lowerBoundSize; ++i) {
                    builder.AddValue(makeValue(keyRange.first[i], i));
                }
            }
            range.first = rowBuffer->Capture(builder.GetRow());
            builder.Reset();


            for (size_t i = 0; i < offset; ++i) {
                builder.AddValue(prefix[i]);
            }

            if (refineUpper) {
                for (size_t i = offset; i < upperBoundSize; ++i) {
                    builder.AddValue(makeValue(keyRange.second[i], i));
                }
            } else {
                builder.AddValue(MakeUnversionedSentinelValue(EValueType::Max));
            }
            range.second = rowBuffer->Capture(builder.GetRow());
            builder.Reset();

            if (insertUndefined || !IsEmpty(range)) {
                result->push_back(range);
            }
        }
        return;
    }

    YCHECK(trie);
    YCHECK(trie->Offset == offset);

    const auto& resultBounds = trie->Bounds;
    YCHECK(!(resultBounds.size() & 1));

    for (size_t i = 0; i + 1 < resultBounds.size(); i += 2) {
        auto lower = resultBounds[i];
        auto upper = resultBounds[i + 1];

        YCHECK(CompareBound(lower, upper, true, false) < 0);

        auto keyRangeLowerBound = TBound(keyRange.first[offset], true);
        auto keyRangeUpperBound = TBound(keyRange.second[offset], offset + 1 < upperBoundSize);

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

        std::pair<TRow, TRow> range;
        for (size_t j = 0; j < offset; ++j) {
            builder.AddValue(prefix[j]);
        }

        if (lowerBoundRefined) {
            for (size_t j = offset; j < lowerBoundSize; ++j) {
                builder.AddValue(makeValue(keyRange.first[j], j));
            }
        } else {
            builder.AddValue(lower.Value);

            if (!lower.Included) {
                builder.AddValue(MakeUnversionedSentinelValue(EValueType::Max));
            }
        }
        
        range.first = rowBuffer->Capture(builder.GetRow());
        builder.Reset();

        for (size_t j = 0; j < offset; ++j) {
            builder.AddValue(prefix[j]);
        }

        if (upperBoundRefined) {
            for (size_t j = offset; j < upperBoundSize; ++j) {
                builder.AddValue(makeValue(keyRange.second[j], j));
            }
        } else {
            builder.AddValue(upper.Value);

            if (upper.Included) {
                builder.AddValue(MakeUnversionedSentinelValue(EValueType::Max));
            }
        }

        range.second = rowBuffer->Capture(builder.GetRow());
        builder.Reset();
        result->push_back(range);
    }


    prefix.emplace_back();

    for (const auto& next : trie->Next) {
        auto value = makeValue(next.first, offset);

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
            next.second,
            result,
            rowBuffer,
            insertUndefined,
            prefix,
            refineLowerNext,
            refineUpperNext);
    }
}

TRowRanges GetRangesFromTrieWithinRange(
    const TRowRange& keyRange,
    TKeyTriePtr trie,
    TRowBufferPtr rowBuffer,
    bool insertUndefined)
{
    TRowRanges result;
    GetRangesFromTrieWithinRangeImpl(keyRange, trie, &result, rowBuffer, insertUndefined);
    return insertUndefined ? result : MergeOverlappingRanges(std::move(result));
}

Stroka ToString(TKeyTriePtr node) {
    auto printOffset = [](int offset) {
        Stroka str;
        for (int i = 0; i < offset; ++i) {
            str += "  ";
        }
        return str;
    };

    std::function<Stroka(TKeyTriePtr, size_t)> printNode =
        [&](TKeyTriePtr node, size_t offset) {
            Stroka str;
            str += printOffset(offset);

            if (!node) {
                str += "(universe)";
            } else {
                str += "(key";
                str += NYT::ToString(node->Offset);
                str += ", { ";

                for (int i = 0; i < node->Bounds.size(); i += 2) {
                    str += node->Bounds[i].Included ? "[" : "(";
                    str += ToString(node->Bounds[i].Value);
                    str += ":";
                    str += ToString(node->Bounds[i+1].Value);
                    str += node->Bounds[i+1].Included ? "]" : ")";
                    if (i + 2 < node->Bounds.size()) {
                        str += ", ";
                    }
                }

                str += " })";

                for (const auto& next : node->Next) {
                    str += "\n";
                    str += printOffset(node->Offset);
                    str += ToString(next.first);
                    str += ":\n";
                    str += printNode(next.second, offset + 1);
                }
            }
            return str;
        };

    return printNode(node, 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
