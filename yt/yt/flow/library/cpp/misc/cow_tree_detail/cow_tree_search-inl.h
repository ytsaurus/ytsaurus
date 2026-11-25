#ifndef COW_TREE_SEARCH_INL_H
    #error "Direct inclusion of this file is not allowed, include cow_tree_search.h"
    // For the sake of sane code completion.
    #include "cow_tree_search.h"
#endif

namespace NYT::NFlow::NCowTreeInternals {

////////////////////////////////////////////////////////////////////////////////

template <class TAnyNode>
int TPathItem<TAnyNode>::NodeSize() const
{
    return Node->Size;
}

template <class TAnyNode>
typename TAnyNode::TKey& TPathItem<TAnyNode>::PathKey() const
{
    return Node->Keys[Pos].Get();
}

////////////////////////////////////////////////////////////////////////////////

template <class TConfig>
template <class TAnyKey>
typename TCowTreeSearch<TConfig>::TKeyHolder* TCowTreeSearch<TConfig>::BranchlessBinary(const TLess& less, const TAnyKey& key, TKeyHolder* first, TKeyHolder* last)
{
    auto count = last - first;
    while (count > 0) {
        auto half = count / 2;
        if (less(first[half].Get(), key)) {
            first += half + 1;
            count -= half + 1;
        } else {
            count = half;
        }
    }
    return first;
}

template <class TConfig>
template <class TAnyKey>
typename TCowTreeSearch<TConfig>::TKeyHolder* TCowTreeSearch<TConfig>::BranchyBinary(const TLess& less, const TAnyKey& key, TKeyHolder* first, TKeyHolder* last)
{
    // Unsigned index halving, the absl shape: the pointer form needs a signed
    // division fix-up on every step, and in a latency-bound loop the shorter body
    // lets the out-of-order window overlap more of the next lookup.
    size_t low = 0;
    size_t high = last - first;
    while (low != high) {
        const size_t middle = (low + high) >> 1;
        if (less(first[middle].Get(), key)) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    return first + low;
}

template <class TConfig>
template <class TAnyKey>
typename TCowTreeSearch<TConfig>::TKeyHolder* TCowTreeSearch<TConfig>::HybridLinear(const TLess& less, const TAnyKey& key, TKeyHolder* first, TKeyHolder* last)
{
    size_t low = 0;
    size_t high = last - first;
    while (high - low > static_cast<size_t>(LinearSearchLimit)) {
        const size_t middle = (low + high) >> 1;
        if (less(first[middle].Get(), key)) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    while (low != high && less(first[low].Get(), key)) {
        ++low;
    }
    return first + low;
}

template <class TConfig>
template <class TAnyKey>
typename TCowTreeSearch<TConfig>::TKeyHolder* TCowTreeSearch<TConfig>::UpperBranchlessBinary(const TLess& less, const TAnyKey& key, TKeyHolder* first, TKeyHolder* last)
{
    auto count = last - first;
    while (count > 0) {
        auto half = count / 2;
        if (!less(key, first[half].Get())) {
            first += half + 1;
            count -= half + 1;
        } else {
            count = half;
        }
    }
    return first;
}

template <class TConfig>
template <class TAnyKey>
typename TCowTreeSearch<TConfig>::TKeyHolder* TCowTreeSearch<TConfig>::UpperBranchyBinary(const TLess& less, const TAnyKey& key, TKeyHolder* first, TKeyHolder* last)
{
    size_t low = 0;
    size_t high = last - first;
    while (low != high) {
        const size_t middle = (low + high) >> 1;
        if (!less(key, first[middle].Get())) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    return first + low;
}

template <class TConfig>
template <class TAnyKey>
typename TCowTreeSearch<TConfig>::TKeyHolder* TCowTreeSearch<TConfig>::UpperHybridLinear(const TLess& less, const TAnyKey& key, TKeyHolder* first, TKeyHolder* last)
{
    size_t low = 0;
    size_t high = last - first;
    while (high - low > static_cast<size_t>(LinearSearchLimit)) {
        const size_t middle = (low + high) >> 1;
        if (!less(key, first[middle].Get())) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    while (low != high && !less(key, first[low].Get())) {
        ++low;
    }
    return first + low;
}

template <class TConfig>
template <class TAnyKey>
typename TCowTreeSearch<TConfig>::TKeyHolder* TCowTreeSearch<TConfig>::Lookup(const TLess& less, const TAnyKey& key, TKeyHolder* first, TKeyHolder* last)
{
    if constexpr (ReadSearch == ECowTreeSearch::BranchlessBinary) {
        return BranchlessBinary(less, key, first, last);
    } else if constexpr (ReadSearch == ECowTreeSearch::BranchyBinary) {
        return BranchyBinary(less, key, first, last);
    } else {
        return HybridLinear(less, key, first, last);
    }
}

template <class TConfig>
template <class TAnyKey>
typename TCowTreeSearch<TConfig>::TLookupResult TCowTreeSearch<TConfig>::LookupThreeWay(const TLess& less, const TAnyKey& key, TKeyHolder* first, TKeyHolder* last)
{
    int low = 0;
    int high = last - first;
    while (low != high) {
        const int middle = (low + high) >> 1;
        const auto order = CowTreeCompare(less, first[middle].Get(), key);
        if (order < 0) {
            low = middle + 1;
        } else if (order > 0) {
            high = middle;
        } else {
            return {first + middle, true};
        }
    }
    return {first + low, false};
}

template <class TConfig>
template <class TAnyKey>
typename TCowTreeSearch<TConfig>::TLookupResult TCowTreeSearch<TConfig>::LookupExact(const TLess& less, const TAnyKey& key, TKeyHolder* first, TKeyHolder* last)
{
    if constexpr (ReadSearch == ECowTreeSearch::BranchyBinary && ThreeWay && CCowTreeThreeWayLookup<TLess, TKey, TAnyKey>) {
        return LookupThreeWay(less, key, first, last);
    } else {
        auto* found = Lookup(less, key, first, last);
        return {found, found != last && !less(key, found->Get())};
    }
}

template <class TConfig>
template <bool ThreeWayHits>
typename TCowTreeSearch<TConfig>::TLookupResult TCowTreeSearch<TConfig>::LookupExactForWrite(const TLess& less, const TKey& key, TKeyHolder* first, TKeyHolder* last)
{
    if constexpr (WriteSearch == ECowTreeSearch::BranchyBinary && ThreeWay && ThreeWayHits) {
        return LookupThreeWay(less, key, first, last);
    } else {
        auto* found = LookupForWrite(less, key, first, last);
        return {found, found != last && !less(key, found->Get())};
    }
}

template <class TConfig>
typename TCowTreeSearch<TConfig>::TKeyHolder* TCowTreeSearch<TConfig>::LookupForWrite(const TLess& less, const TKey& key, TKeyHolder* first, TKeyHolder* last)
{
    if constexpr (WriteSearch == ECowTreeSearch::BranchlessBinary) {
        return BranchlessBinary(less, key, first, last);
    } else if constexpr (WriteSearch == ECowTreeSearch::BranchyBinary) {
        return BranchyBinary(less, key, first, last);
    } else {
        return HybridLinear(less, key, first, last);
    }
}

template <class TConfig>
template <class TAnyKey>
typename TCowTreeSearch<TConfig>::TKeyHolder* TCowTreeSearch<TConfig>::LookupUpper(const TLess& less, const TAnyKey& key, TKeyHolder* first, TKeyHolder* last)
{
    if constexpr (ReadSearch == ECowTreeSearch::BranchlessBinary) {
        return UpperBranchlessBinary(less, key, first, last);
    } else if constexpr (ReadSearch == ECowTreeSearch::BranchyBinary) {
        return UpperBranchyBinary(less, key, first, last);
    } else {
        return UpperHybridLinear(less, key, first, last);
    }
}

template <class TConfig>
template <class TAnyKey>
int TCowTreeSearch<TConfig>::Child(const TLess& less, const TAnyKey& key, TInnerNode* inner)
{
    return Lookup(less, key, inner->Keys + 1, inner->Keys + inner->Size) - inner->Keys - 1;
}

template <class TConfig>
template <class TAnyKey>
int TCowTreeSearch<TConfig>::ChildExact(const TLess& less, const TAnyKey& key, TInnerNode* inner, bool* exact)
{
    if constexpr (ReadSearch == ECowTreeSearch::BranchyBinary && ThreeWay && CCowTreeThreeWayLookup<TLess, TKey, TAnyKey>) {
        auto [found, hit] = LookupExact(less, key, inner->Keys + 1, inner->Keys + inner->Size);
        *exact = hit;
        return found - inner->Keys - 1;
    } else {
        *exact = false;
        return Child(less, key, inner);
    }
}

template <class TConfig>
int TCowTreeSearch<TConfig>::ChildForWrite(const TLess& less, const TKey& key, TInnerNode* inner)
{
    return LookupForWrite(less, key, inner->Keys + 1, inner->Keys + inner->Size) - inner->Keys - 1;
}

template <class TConfig>
template <class TAnyKey>
int TCowTreeSearch<TConfig>::ChildUpper(const TLess& less, const TAnyKey& key, TInnerNode* inner)
{
    return LookupUpper(less, key, inner->Keys + 1, inner->Keys + inner->Size) - inner->Keys - 1;
}

template <class TConfig>
template <bool ThreeWayHits>
typename TCowTreeSearch<TConfig>::TPath TCowTreeSearch<TConfig>::FindPath(const TLess& less, TNode* root, int height, const TKey& key)
{
    YT_ASSERT(height <= TPath::MaxHeight);
    TPath path;
    TNode* node = root;
    for (int i = 0; i < height - 1; i++) {
        auto* inner = static_cast<TInnerNode*>(node);
        int pos;
        if constexpr (WriteSearch == ECowTreeSearch::BranchyBinary && ThreeWay && ThreeWayHits) {
            auto [separator, exact] = LookupThreeWay(less, key, inner->Keys + 1, inner->Keys + inner->Size);
            pos = separator - inner->Keys - 1;
            if (exact) {
                path.Inner[i].Node = inner;
                path.Inner[i].Pos = pos;
                if (pos + 1 < inner->Size) {
                    path.MaxSlotLevel = i;
                    path.MaxSlotIndex = pos + 1;
                }
                node = inner->Children[pos];
                for (int j = i + 1; j < height - 1; j++) {
                    auto* below = static_cast<TInnerNode*>(node);
                    path.Inner[j].Node = below;
                    path.Inner[j].Pos = below->Size - 1;
                    node = below->Children[below->Size - 1];
                }
                auto* leaf = static_cast<TLeafNode*>(node);
                path.Leaf.Node = leaf;
                path.Leaf.Pos = leaf->Size - 1;
                path.Exact = true;
                return path;
            }
        } else {
            pos = ChildForWrite(less, key, inner);
        }
        path.Inner[i].Node = inner;
        path.Inner[i].Pos = pos;
        if (pos + 1 < inner->Size) {
            path.MaxSlotLevel = i;
            path.MaxSlotIndex = pos + 1;
        }
        node = inner->Children[pos];
    }
    {
        auto* leaf = static_cast<TLeafNode*>(node);
        auto [found, exact] = LookupExactForWrite<ThreeWayHits>(less, key, leaf->Keys, leaf->Keys + node->Size);
        path.Leaf.Node = leaf;
        path.Leaf.Pos = found - leaf->Keys;
        path.Exact = exact;
    }
    return path;
}

template <class TConfig>
typename TCowTreeSearch<TConfig>::TPath TCowTreeSearch<TConfig>::FindRightmostPath(TNode* root, int height)
{
    YT_ASSERT(height <= TPath::MaxHeight);
    TPath path;
    TNode* node = root;
    for (int i = 0; i < height - 1; i++) {
        auto* inner = static_cast<TInnerNode*>(node);
        path.Inner[i].Node = inner;
        path.Inner[i].Pos = inner->Size - 1;
        node = inner->Children[inner->Size - 1];
    }
    auto* leaf = static_cast<TLeafNode*>(node);
    path.Leaf.Node = leaf;
    path.Leaf.Pos = leaf->Size;
    return path;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCowTreeInternals
