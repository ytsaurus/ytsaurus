#pragma once

#include "cow_tree_config.h"
#include "cow_tree_node.h"
#include "cow_tree_value.h"

#include <library/cpp/yt/assert/assert.h>

#include <util/system/types.h>

namespace NYT::NFlow::NCowTreeInternals {

////////////////////////////////////////////////////////////////////////////////

//! One step of a root-to-leaf descent.
template <class TAnyNode>
struct TPathItem
{
    TAnyNode* Node = nullptr;
    int Pos = 0;

    int NodeSize() const;
    typename TAnyNode::TKey& PathKey() const;
};

//! A full root-to-leaf descent.
template <class TConfig>
struct TCowTreePath
{
    static constexpr int MaxHeight = ComputeCowTreeMaxHeight(TConfig::MaxNodeSize, TConfig::MaxSize);

    TPathItem<TInner<TConfig>> Inner[MaxHeight - 1];
    TPathItem<TLeaf<TConfig>> Leaf;

    //! The separator of the deepest subtree whose rightmost leaf ends the path: the one
    //! extra place holding that leaf's max (the tree's max holder for the rightmost path).
    //! Addressed by (level, slot), so it survives copy-on-write path normalization.
    int MaxSlotLevel = -1;
    int MaxSlotIndex = 0;

    //! Whether the leaf position holds the key itself; known without a further
    //! comparison only under a three-way comparator (see TCowTreeSearch::ThreeWay).
    bool Exact = false;
};

//! The in-node search flavours (see ECowTreeSearch) and the descents built on them.
//! Each flavour is optimized for its own key regime and never mixed with the others,
//! so ui64, pointer and string keys can be tuned independently; Lookup/LookupForWrite/
//! LookupUpper dispatch on the compile-time flavour choice of the key and comparator.
template <class TConfig>
class TCowTreeSearch
{
public:
    using TKey = typename TConfig::TKey;
    using TLess = typename TConfig::TLess;
    using TKeyHolder = TValueHolder<TKey>;
    using TInnerNode = TInner<TConfig>;
    using TLeafNode = TLeaf<TConfig>;
    using TPath = TCowTreePath<TConfig>;

    static constexpr int LinearSearchLimit = TConfig::LinearSearchLimit;
    static constexpr ECowTreeSearch ReadSearch = CowTreeReadSearch<TKey, TLess>();
    static constexpr ECowTreeSearch WriteSearch = CowTreeWriteSearch<TKey, TLess>();
    //! Whether a descent may stop at an exact hit (see CowTreeComparesThreeWay).
    static constexpr bool ThreeWay = CowTreeComparesThreeWay<TKey, TLess>();

    //! A lower-bound position and whether it holds the key itself.
    struct TLookupResult
    {
        TKeyHolder* Position;
        bool Exact;
    };

    // The read-side searches take any key type the comparator accepts (std-style
    // heterogeneous lookup); the write side stays keyed by TKey.

    template <class TAnyKey>
    static TKeyHolder* BranchlessBinary(const TLess& less, const TAnyKey& key, TKeyHolder* first, TKeyHolder* last);
    template <class TAnyKey>
    static TKeyHolder* BranchyBinary(const TLess& less, const TAnyKey& key, TKeyHolder* first, TKeyHolder* last);
    template <class TAnyKey>
    static TKeyHolder* HybridLinear(const TLess& less, const TAnyKey& key, TKeyHolder* first, TKeyHolder* last);
    template <class TAnyKey>
    static TKeyHolder* UpperBranchlessBinary(const TLess& less, const TAnyKey& key, TKeyHolder* first, TKeyHolder* last);
    template <class TAnyKey>
    static TKeyHolder* UpperBranchyBinary(const TLess& less, const TAnyKey& key, TKeyHolder* first, TKeyHolder* last);
    template <class TAnyKey>
    static TKeyHolder* UpperHybridLinear(const TLess& less, const TAnyKey& key, TKeyHolder* first, TKeyHolder* last);

    template <class TAnyKey>
    static TKeyHolder* Lookup(const TLess& less, const TAnyKey& key, TKeyHolder* first, TKeyHolder* last);
    template <class TAnyKey>
    static TLookupResult LookupThreeWay(const TLess& less, const TAnyKey& key, TKeyHolder* first, TKeyHolder* last);
    //! Lookup that also answers whether the key is present: with a three-way
    //! comparator by stopping at the hit, otherwise by one more comparison.
    template <class TAnyKey>
    static TLookupResult LookupExact(const TLess& less, const TAnyKey& key, TKeyHolder* first, TKeyHolder* last);
    //! The write-side twin; |ThreeWayHits| = false keeps the two-way halving and
    //! answers the presence by one more comparison — the shape for a key that is
    //! most likely absent (an insert), where a hit can not end the search early
    //! and the three outcomes per step are pure overhead.
    template <bool ThreeWayHits>
    static TLookupResult LookupExactForWrite(const TLess& less, const TKey& key, TKeyHolder* first, TKeyHolder* last);
    static TKeyHolder* LookupForWrite(const TLess& less, const TKey& key, TKeyHolder* first, TKeyHolder* last);
    template <class TAnyKey>
    static TKeyHolder* LookupUpper(const TLess& less, const TAnyKey& key, TKeyHolder* first, TKeyHolder* last);

    //! Routes |key| within an inner node: the first separator >= key wins its left
    //! child; the upper-bound route takes the first separator > key.
    template <class TAnyKey>
    static int Child(const TLess& less, const TAnyKey& key, TInnerNode* inner);
    //! Child that also reports a three-way hit on a separator: the key is then the
    //! last element of the child chosen. Never a hit without a three-way comparator.
    template <class TAnyKey>
    static int ChildExact(const TLess& less, const TAnyKey& key, TInnerNode* inner, bool* exact);
    static int ChildForWrite(const TLess& less, const TKey& key, TInnerNode* inner);
    template <class TAnyKey>
    static int ChildUpper(const TLess& less, const TAnyKey& key, TInnerNode* inner);

    //! The write descent; with |ThreeWayHits| under a three-way comparator a
    //! separator hit ends the comparisons and the path continues along the
    //! rightmost edge of the child (the key is that subtree's last element).
    //! Exact tells whether the leaf position holds the key either way.
    template <bool ThreeWayHits>
    static TPath FindPath(const TLess& less, TNode* root, int height, const TKey& key);

    //! The comparison-free descent for keys above the tree max: rightmost child at
    //! every level, past-the-end position in the rightmost leaf. MaxSlot stays at
    //! the tree's max holder, as for any rightmost path.
    static TPath FindRightmostPath(TNode* root, int height);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCowTreeInternals

#define COW_TREE_SEARCH_INL_H
#include "cow_tree_search-inl.h"
#undef COW_TREE_SEARCH_INL_H
