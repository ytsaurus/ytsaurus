#ifndef COW_TREE_INL_H
    #error "Direct inclusion of this file is not allowed, include cow_tree.h"
    // For the sake of sane code completion.
    #include "cow_tree.h"
#endif

#include <library/cpp/yt/assert/assert.h>

#include <util/generic/scope.h>

#include <algorithm>
#include <cstring>
#include <optional>
#include <tuple>
#include <vector>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <CCowTreeConfig TConfig>
void TCowTreeBase<TConfig>::TNodeOps::Ref(TNode* node)
{
    NCowTreeInternals::RefNode<TConfig>(node);
}

template <CCowTreeConfig TConfig>
void TCowTreeBase<TConfig>::TNodeOps::Unref(TNode* node)
{
    NCowTreeInternals::UnrefNode<TConfig>(node);
}

template <CCowTreeConfig TConfig>
void TCowTreeBase<TConfig>::TCoreOps::Ref(TCore* core)
{
    core->RefCounter.Ref();
}

template <CCowTreeConfig TConfig>
void TCowTreeBase<TConfig>::TCoreOps::Unref(TCore* core)
{
    if (core->RefCounter.Unref()) {
        NCowTreeInternals::DeleteCowTreeNode<TConfig>(core);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <CCowTreeConfig TConfig>
TCowTreeBase<TConfig>::TCowTreeBase(const TLess& less)
    : Less_(less)
{ }

template <CCowTreeConfig TConfig>
TCowTree<TConfig>::TCowTree()
    : TCowTree(TLess())
{ }

template <CCowTreeConfig TConfig>
TCowTree<TConfig>::TCowTree(const TLess& less)
    : TBase(less)
{
    Core_ = TCoreRef(NCowTreeInternals::CreateCowTreeNode<TConfig, TCore>());
}

template <CCowTreeConfig TConfig>
TCowTree<TConfig>::TCowTree(std::initializer_list<value_type> elements, const TLess& less)
    : TCowTree(less)
{
    insert(elements);
}

template <CCowTreeConfig TConfig>
TCowTreeSnapshot<TConfig>::TCowTreeSnapshot(const TCowTreeSnapshot& other)
    : TCowTreeSnapshot(TSnapshotTag{}, other, other.ViewLink_.SnapshotVersion)
{ }

template <CCowTreeConfig TConfig>
TCowTreeSnapshot<TConfig>::TCowTreeSnapshot(TSnapshotTag, const TBase& source, ui64 snapshotVersion)
    : TBase(TSnapshotTag{}, source, snapshotVersion)
{ }

template <CCowTreeConfig TConfig>
TCowTreeBase<TConfig>::TCowTreeBase(TSnapshotTag, const TCowTreeBase& source, ui64 snapshotVersion)
    : Core_(source.Core_)
    , Root_(source.Root_)
    , Size_(source.Size_)
    , Height_(source.Height_)
    , Less_(source.Less_)
{
    if (Height_ > 0) {
        MaxKeyHolder_.CopyFrom(source.MaxKeyHolder_);
    }
    ViewLink_.IsSnapshot = true;
    ViewLink_.SnapshotVersion = snapshotVersion;
    Core_->RegisterSnapshot(&ViewLink_, &source.ViewLink_);
}

template <CCowTreeConfig TConfig>
TCowTreeBase<TConfig>::~TCowTreeBase()
{
#ifndef NDEBUG
    if (Root_.Get() && IsMaster()) {
        AssertNoTrackedIterators(Root_.Get(), Height_ - 1);
    }
#endif
    if (ViewLink_.IsSnapshot) {
        Core_->UnregisterSnapshot(&ViewLink_);
    }
    if (Height_ > 0) {
        MaxKeyHolder_.Clear();
    }
    Root_.Reset();
}

template <CCowTreeConfig TConfig>
bool TCowTreeBase<TConfig>::IsMaster() const
{
    return !ViewLink_.IsSnapshot;
}

template <CCowTreeConfig TConfig>
ui64 TCowTreeBase<TConfig>::GetVersion() const
{
    return IsMaster() ? Core_->Version : ViewLink_.SnapshotVersion;
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::Snapshot() const -> TSnapshot
{
    return TSnapshot(TSnapshotTag{}, *this, Core_->Version);
}

////////////////////////////////////////////////////////////////////////////////

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::LeafIterators(TLeaf* leaf) -> TIterator*
{
    return static_cast<TIterator*>(leaf->TrackedIterators);
}

template <CCowTreeConfig TConfig>
void TCowTree<TConfig>::FixLeafInsert(TLeaf* leaf, int insertPos)
{
    for (auto* it = LeafIterators(leaf); it != nullptr; it = it->NextTracked_) {
        if (it->Pos_ >= insertPos) {
            ++it->Pos_;
        }
    }
}

template <CCowTreeConfig TConfig>
void TCowTree<TConfig>::FixLeafErase(TLeaf* leaf, int erasePos)
{
    // An iterator on the erased element stays put: the successor slides into its
    // position (the no-successor-in-this-leaf case is detached beforehand).
    for (auto* it = LeafIterators(leaf); it != nullptr; it = it->NextTracked_) {
        if (it->Pos_ > erasePos) {
            --it->Pos_;
        }
    }
}

template <CCowTreeConfig TConfig>
void TCowTree<TConfig>::MoveLeafIterators(TLeaf* from, TLeaf* to, int insertPos, int skipPos)
{
    auto* it = LeafIterators(from);
    from->TrackedIterators = nullptr;
    while (it != nullptr) {
        auto* next = it->NextTracked_;
        it->Leaf_ = to;
        if (skipPos >= 0 && it->Pos_ > skipPos) {
            --it->Pos_;
        }
        if (insertPos >= 0 && it->Pos_ >= insertPos) {
            ++it->Pos_;
        }
        it->LinkToLeaf();
        it = next;
    }
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::DetachIteratorsAt(TLeaf* leaf, int pos) -> TIterator*
{
    TIterator* detached = nullptr;
    auto* it = LeafIterators(leaf);
    while (it != nullptr) {
        auto* next = it->NextTracked_;
        if (it->Pos_ == pos) {
            it->UnlinkFromLeaf();
            it->Leaf_ = nullptr;
            it->Parent_ = nullptr;
            it->PrevTracked_ = nullptr;
            it->NextTracked_ = detached;
            detached = it;
        }
        it = next;
    }
    return detached;
}

template <CCowTreeConfig TConfig>
void TCowTree<TConfig>::AdvanceLastLeafIterators(const TPath& path)
{
    auto* strand = DetachIteratorsAt(path.Leaf.Node, path.Leaf.Pos);
    if (!strand) {
        return;
    }
    if (path.MaxSlotLevel < 0) {
        while (strand) {
            auto* next = strand->NextTracked_;
            *strand = TIterator(this);
            strand = next;
        }
        return;
    }

    // The separator following this leaf's maximum leads to its successor.
    auto* parent = path.Inner[path.MaxSlotLevel].Node;
    int slot = path.MaxSlotIndex;
    for (int level = path.MaxSlotLevel; level < Height_ - 2; ++level) {
        parent = static_cast<TInner*>(parent->Children[slot]);
        slot = 0;
    }
    ReseatStrandToNextLeaf(strand, parent, slot);
}

template <CCowTreeConfig TConfig>
void TCowTree<TConfig>::ReseatStrandToNextLeaf(TIterator* strand, TInner* parent, int slot)
{
    auto* leaf = static_cast<TLeaf*>(parent->Children[slot]);
    YT_ASSERT(leaf->IsLeaf && leaf->Size > 0);
    while (strand) {
        auto* next = strand->NextTracked_;
        strand->Leaf_ = leaf;
        strand->Pos_ = 0;
        strand->Parent_ = parent;
        strand->ParentPos_ = slot;
        strand->LinkToLeaf();
        strand = next;
    }
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::CollectGroupIterators(TNode* const* group, int groupSize, int shiftFrom, int shiftBy) -> TIterator*
{
    TIterator* chain = nullptr;
    int prefix = 0;
    for (int i = 0; i < groupSize; ++i) {
        auto* leaf = static_cast<TLeaf*>(group[i]);
        auto* it = LeafIterators(leaf);
        leaf->TrackedIterators = nullptr;
        while (it != nullptr) {
            auto* next = it->NextTracked_;
            int streamPos = prefix + it->Pos_;
            if (streamPos >= shiftFrom) {
                streamPos += shiftBy;
            }
            it->Pos_ = streamPos;
            it->PrevTracked_ = nullptr;
            it->NextTracked_ = chain;
            chain = it;
            it = next;
        }
        prefix += leaf->Size;
    }
    return chain;
}

template <CCowTreeConfig TConfig>
void TCowTree<TConfig>::DistributeGroupIterators(TIterator* chain, TNode* const* group, int groupSize)
{
    while (chain != nullptr) {
        auto* next = chain->NextTracked_;
        int streamPos = chain->Pos_;
        int i = 0;
        while (streamPos >= group[i]->Size) {
            streamPos -= group[i]->Size;
            ++i;
            YT_ASSERT(i < groupSize);
        }
        chain->Leaf_ = static_cast<TLeaf*>(group[i]);
        chain->Pos_ = streamPos;
        chain->LinkToLeaf();
        chain = next;
    }
}

template <CCowTreeConfig TConfig>
void TCowTree<TConfig>::ReparentLeafChildren(TInner* parent)
{
    for (int i = 0; i < parent->Size; ++i) {
        auto* leaf = static_cast<TLeaf*>(parent->Children[i]);
        YT_ASSERT(leaf->IsLeaf);
        for (auto* it = LeafIterators(leaf); it != nullptr; it = it->NextTracked_) {
            it->Parent_ = parent;
            it->ParentPos_ = i;
        }
    }
}

template <CCowTreeConfig TConfig>
void TCowTree<TConfig>::SetLeafParent(TLeaf* leaf, TInner* parent, int parentPos)
{
    for (auto* it = LeafIterators(leaf); it != nullptr; it = it->NextTracked_) {
        it->Parent_ = parent;
        it->ParentPos_ = parentPos;
    }
}

template <CCowTreeConfig TConfig>
void TCowTree<TConfig>::EvictLeafIterators(TNode* node, int moreLevels)
{
    if (moreLevels == 0) {
        auto* leaf = static_cast<TLeaf*>(node);
        auto* it = LeafIterators(leaf);
        leaf->TrackedIterators = nullptr;
        while (it != nullptr) {
            auto* next = it->NextTracked_;
            it->Leaf_ = nullptr;
            it->Parent_ = nullptr;
            it->Tracking_ = false;
            it->PrevTracked_ = nullptr;
            it->NextTracked_ = nullptr;
            it = next;
        }
        return;
    }
    auto* inner = static_cast<TInner*>(node);
    for (int i = 0; i < inner->Size; ++i) {
        EvictLeafIterators(inner->Children[i], moreLevels - 1);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <CCowTreeConfig TConfig>
auto TCowTreeBase<TConfig>::begin() const -> const_iterator
{
    return const_iterator(const_cast<TCowTreeBase*>(this)->SeekFirst());
}

template <CCowTreeConfig TConfig>
auto TCowTreeBase<TConfig>::end() const -> const_iterator
{
    return const_iterator(const_cast<TCowTreeBase*>(this));
}

template <CCowTreeConfig TConfig>
auto TCowTreeBase<TConfig>::cbegin() const -> const_iterator
{
    return begin();
}

template <CCowTreeConfig TConfig>
auto TCowTreeBase<TConfig>::cend() const -> const_iterator
{
    return end();
}

template <CCowTreeConfig TConfig>
auto TCowTreeBase<TConfig>::rbegin() const -> const_reverse_iterator
{
    return std::make_reverse_iterator(end());
}

template <CCowTreeConfig TConfig>
auto TCowTreeBase<TConfig>::rend() const -> const_reverse_iterator
{
    return std::make_reverse_iterator(begin());
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::begin() -> iterator
{
    return iterator(SeekFirst());
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::end() -> iterator
{
    return iterator(this);
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::rbegin() -> reverse_iterator
{
    return std::make_reverse_iterator(end());
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::rend() -> reverse_iterator
{
    return std::make_reverse_iterator(begin());
}

template <CCowTreeConfig TConfig>
size_t TCowTreeBase<TConfig>::size() const
{
    return Size_;
}

template <CCowTreeConfig TConfig>
bool TCowTreeBase<TConfig>::empty() const
{
    return Size_ == 0;
}

template <CCowTreeConfig TConfig>
auto TCowTreeBase<TConfig>::MakeIterator(TLeaf* leaf, int pos, TInner* parent, int parentPos) -> TIterator
{
    return TIterator(this, leaf, pos, parent, parentPos);
}

template <CCowTreeConfig TConfig>
auto TCowTreeBase<TConfig>::SeekFirst() -> TIterator
{
    if (COW_TREE_UNLIKELY(Height_ == 0)) {
        return end();
    }
    TNode* node = Root_.Get();
    TInner* parent = nullptr;
    for (int i = 0; i < Height_ - 1; i++) {
        parent = static_cast<TInner*>(node);
        node = parent->Children[0];
    }
    return MakeIterator(static_cast<TLeaf*>(node), 0, parent, 0);
}

template <CCowTreeConfig TConfig>
auto TCowTreeBase<TConfig>::SeekLast() -> TIterator
{
    if (COW_TREE_UNLIKELY(Height_ == 0)) {
        return end();
    }
    TNode* node = Root_.Get();
    TInner* parent = nullptr;
    int parentPos = 0;
    for (int i = 0; i < Height_ - 1; i++) {
        parent = static_cast<TInner*>(node);
        parentPos = parent->Size - 1;
        node = parent->Children[parentPos];
    }
    auto* leaf = static_cast<TLeaf*>(node);
    return MakeIterator(leaf, leaf->Size - 1, parent, parentPos);
}

template <CCowTreeConfig TConfig>
template <class TAnyKey>
auto TCowTreeBase<TConfig>::SeekFirstGreaterOrEqual(const TAnyKey& key) -> TIterator
{
    if (COW_TREE_UNLIKELY(Height_ == 0)) {
        return end();
    }
    TNode* node = Root_.Get();
    TInner* parent = nullptr;
    int parentPos = 0;
    for (int i = 0; i < Height_ - 1; i++) {
        parent = static_cast<TInner*>(node);
        parentPos = TSearch::Child(Less_, key, parent);
        node = parent->Children[parentPos];
    }
    auto* leaf = static_cast<TLeaf*>(node);
    int pos = TSearch::Lookup(Less_, key, leaf->Keys, leaf->Keys + leaf->Size) - leaf->Keys;
    // Routing guarantees leaf max >= key everywhere but on the rightmost path.
    if (COW_TREE_UNLIKELY(pos == leaf->Size)) {
        return end();
    }
    return MakeIterator(leaf, pos, parent, parentPos);
}

template <CCowTreeConfig TConfig>
template <class TAnyKey>
auto TCowTreeBase<TConfig>::SeekFirstGreater(const TAnyKey& key) -> TIterator
{
    if (COW_TREE_UNLIKELY(Height_ == 0)) {
        return end();
    }
    TNode* node = Root_.Get();
    TInner* parent = nullptr;
    int parentPos = 0;
    for (int i = 0; i < Height_ - 1; i++) {
        parent = static_cast<TInner*>(node);
        parentPos = TSearch::ChildUpper(Less_, key, parent);
        node = parent->Children[parentPos];
    }
    auto* leaf = static_cast<TLeaf*>(node);
    int pos = TSearch::LookupUpper(Less_, key, leaf->Keys, leaf->Keys + leaf->Size) - leaf->Keys;
    if (COW_TREE_UNLIKELY(pos == leaf->Size)) {
        return end();
    }
    return MakeIterator(leaf, pos, parent, parentPos);
}

template <CCowTreeConfig TConfig>
template <class TAnyKey>
auto TCowTreeBase<TConfig>::SeekLastLess(const TAnyKey& key) -> TIterator
{
    if (COW_TREE_UNLIKELY(Height_ == 0)) {
        return end();
    }
    TNode* node = Root_.Get();
    TInner* parent = nullptr;
    int parentPos = 0;
    const TKey* fallback = nullptr;
    for (int i = 0; i < Height_ - 1; i++) {
        parent = static_cast<TInner*>(node);
        parentPos = TSearch::Child(Less_, key, parent);
        if (parentPos >= 1) {
            fallback = &parent->Keys[parentPos].Get();
        }
        node = parent->Children[parentPos];
    }
    auto* leaf = static_cast<TLeaf*>(node);
    int pos = TSearch::Lookup(Less_, key, leaf->Keys, leaf->Keys + leaf->Size) - leaf->Keys;
    if (COW_TREE_LIKELY(pos > 0)) {
        return MakeIterator(leaf, pos - 1, parent, parentPos);
    }
    if (!fallback) {
        return end();
    }
    return SeekFirstGreaterOrEqual(*fallback);
}

template <CCowTreeConfig TConfig>
const typename TConfig::TKey* TCowTreeBase<TConfig>::FindKeyGreaterOrEqual(const TKey& key) const
{
    if (COW_TREE_UNLIKELY(Height_ == 0)) {
        return nullptr;
    }
    TNode* node = Root_.Get();
    for (int i = 0; i < Height_ - 1; i++) {
        auto* inner = static_cast<TInner*>(node);
        node = inner->Children[TSearch::Child(Less_, key, inner)];
    }
    auto* leaf = static_cast<TLeaf*>(node);
    auto* found = TSearch::Lookup(Less_, key, leaf->Keys, leaf->Keys + leaf->Size);
    if (COW_TREE_UNLIKELY(found == leaf->Keys + leaf->Size)) {
        return nullptr;
    }
    return &found->Get();
}

template <CCowTreeConfig TConfig>
template <class TAnyKey>
auto TCowTreeBase<TConfig>::DoFind(const TAnyKey& key) const -> TIterator
{
    if (Height_ == 0) {
        return end();
    }
    TNode* node = Root_.Get();
    TInner* parent = nullptr;
    int parentPos = 0;
    for (int i = 0; i < Height_ - 1; i++) {
        parent = static_cast<TInner*>(node);
        bool exact = false;
        parentPos = TSearch::ChildExact(Less_, key, parent, &exact);
        node = parent->Children[parentPos];
        if (exact) {
            // A separator hit: the key is the last element of the subtree just
            // entered, reached along its rightmost edge without a comparison.
            for (int j = i + 1; j < Height_ - 1; j++) {
                parent = static_cast<TInner*>(node);
                parentPos = parent->Size - 1;
                node = parent->Children[parentPos];
            }
            auto* leaf = static_cast<TLeaf*>(node);
            return const_cast<TCowTreeBase*>(this)->MakeIterator(leaf, leaf->Size - 1, parent, parentPos);
        }
    }
    {
        auto* leaf = static_cast<TLeaf*>(node);
        auto [found, exact] = TSearch::LookupExact(Less_, key, leaf->Keys, leaf->Keys + node->Size);
        if (exact) {
            return const_cast<TCowTreeBase*>(this)->MakeIterator(leaf, found - leaf->Keys, parent, parentPos);
        }
    }
    return end();
}

template <CCowTreeConfig TConfig>
template <class TAnyKey>
bool TCowTreeBase<TConfig>::DoContains(const TAnyKey& key) const
{
    if (Height_ == 0) {
        return false;
    }
    TNode* node = Root_.Get();
    for (int i = 0; i < Height_ - 1; i++) {
        auto* inner = static_cast<TInner*>(node);
        bool exact = false;
        node = inner->Children[TSearch::ChildExact(Less_, key, inner, &exact)];
        if (exact) {
            return true;
        }
    }
    auto* leaf = static_cast<TLeaf*>(node);
    return TSearch::LookupExact(Less_, key, leaf->Keys, leaf->Keys + leaf->Size).Exact;
}

template <CCowTreeConfig TConfig>
auto TCowTreeBase<TConfig>::find(const TKey& key) const -> const_iterator
{
    return const_iterator(DoFind(key));
}

template <CCowTreeConfig TConfig>
auto TCowTreeBase<TConfig>::lower_bound(const TKey& key) const -> const_iterator
{
    return const_iterator(const_cast<TCowTreeBase*>(this)->SeekFirstGreaterOrEqual(key));
}

template <CCowTreeConfig TConfig>
auto TCowTreeBase<TConfig>::upper_bound(const TKey& key) const -> const_iterator
{
    return const_iterator(const_cast<TCowTreeBase*>(this)->SeekFirstGreater(key));
}

template <CCowTreeConfig TConfig>
bool TCowTreeBase<TConfig>::contains(const TKey& key) const
{
    return DoContains(key);
}

template <CCowTreeConfig TConfig>
size_t TCowTreeBase<TConfig>::count(const TKey& key) const
{
    return DoContains(key) ? 1 : 0;
}

template <CCowTreeConfig TConfig>
template <class TAnyKey>
auto TCowTreeBase<TConfig>::find(const TAnyKey& key) const -> const_iterator
    requires CCowTreeTransparentLess<typename TConfig::TLess>
{
    return const_iterator(DoFind(key));
}

template <CCowTreeConfig TConfig>
template <class TAnyKey>
auto TCowTreeBase<TConfig>::lower_bound(const TAnyKey& key) const -> const_iterator
    requires CCowTreeTransparentLess<typename TConfig::TLess>
{
    return const_iterator(const_cast<TCowTreeBase*>(this)->SeekFirstGreaterOrEqual(key));
}

template <CCowTreeConfig TConfig>
template <class TAnyKey>
auto TCowTreeBase<TConfig>::upper_bound(const TAnyKey& key) const -> const_iterator
    requires CCowTreeTransparentLess<typename TConfig::TLess>
{
    return const_iterator(const_cast<TCowTreeBase*>(this)->SeekFirstGreater(key));
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::find(const TKey& key) -> iterator
{
    return iterator(DoFind(key));
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::lower_bound(const TKey& key) -> iterator
{
    return iterator(SeekFirstGreaterOrEqual(key));
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::upper_bound(const TKey& key) -> iterator
{
    return iterator(SeekFirstGreater(key));
}

template <CCowTreeConfig TConfig>
template <class TAnyKey>
auto TCowTree<TConfig>::find(const TAnyKey& key) -> iterator
    requires CCowTreeTransparentLess<typename TConfig::TLess>
{
    return iterator(DoFind(key));
}

template <CCowTreeConfig TConfig>
template <class TAnyKey>
auto TCowTree<TConfig>::lower_bound(const TAnyKey& key) -> iterator
    requires CCowTreeTransparentLess<typename TConfig::TLess>
{
    return iterator(SeekFirstGreaterOrEqual(key));
}

template <CCowTreeConfig TConfig>
template <class TAnyKey>
auto TCowTree<TConfig>::upper_bound(const TAnyKey& key) -> iterator
    requires CCowTreeTransparentLess<typename TConfig::TLess>
{
    return iterator(SeekFirstGreater(key));
}

template <CCowTreeConfig TConfig>
template <class TAnyKey>
bool TCowTreeBase<TConfig>::contains(const TAnyKey& key) const
    requires CCowTreeTransparentLess<typename TConfig::TLess>
{
    return DoContains(key);
}

template <CCowTreeConfig TConfig>
template <class TAnyKey>
size_t TCowTreeBase<TConfig>::count(const TAnyKey& key) const
    requires CCowTreeTransparentLess<typename TConfig::TLess>
{
    return DoContains(key) ? 1 : 0;
}

////////////////////////////////////////////////////////////////////////////////

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::TryMutableValueHolder(const TKey& key) -> NCowTreeInternals::TValueHolder<TValue>*
    requires HasValue
{
    YT_VERIFY(IsMaster());
    if (COW_TREE_UNLIKELY(Height_ == 0)) {
        return nullptr;
    }
    auto path = FindPath(key);
    if (COW_TREE_UNLIKELY(!PathHitsKey(path))) {
        return nullptr;
    }
    return MutableValueAtPath(path);
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::MutableValueHolder(const TKey& key) -> NCowTreeInternals::TValueHolder<TValue>&
    requires HasValue
{
    auto* holder = TryMutableValueHolder(key);
    YT_VERIFY(holder != nullptr);
    return *holder;
}

template <CCowTreeConfig TConfig>
template <class TResult>
auto TCowTree<TConfig>::MutableValueFor(TIterator& position) -> TResult&
    requires HasValue
{
    auto* leaf = position.Leaf_;
    if (COW_TREE_LIKELY(leaf->Version > Core_->GetMaxLiveSnapshotVersion())) {
        ++Core_->Version;
        return leaf->Values[position.Pos_].Get();
    }
    // The full copy-on-write access; the leaf copy repoints |position| to itself.
    return MutableAt(leaf->Keys[position.Pos_].Get());
}

template <CCowTreeConfig TConfig>
template <class TResult>
auto TCowTreeBase<TConfig>::TryReadValue(const TKey& key) const -> const TResult*
    requires HasValue
{
    if (COW_TREE_UNLIKELY(Height_ == 0)) {
        return nullptr;
    }
    TNode* node = Root_.Get();
    for (int i = 0; i < Height_ - 1; i++) {
        auto* inner = static_cast<TInner*>(node);
        bool exact = false;
        node = inner->Children[TSearch::ChildExact(Less_, key, inner, &exact)];
        if (exact) {
            // The key is the last element of the subtree just entered.
            for (int j = i + 1; j < Height_ - 1; j++) {
                auto* below = static_cast<TInner*>(node);
                node = below->Children[below->Size - 1];
            }
            auto* leaf = static_cast<TLeaf*>(node);
            return &leaf->Values[leaf->Size - 1].Get();
        }
    }
    auto* leaf = static_cast<TLeaf*>(node);
    auto [found, exact] = TSearch::LookupExact(Less_, key, leaf->Keys, leaf->Keys + leaf->Size);
    if (!exact) {
        return nullptr;
    }
    return &leaf->Values[found - leaf->Keys].Get();
}

template <CCowTreeConfig TConfig>
template <class TResult>
auto TCowTreeBase<TConfig>::ReadValue(const TKey& key) const -> const TResult&
    requires HasValue
{
    const auto* value = TryReadValue(key);
    YT_VERIFY(value != nullptr);
    return *value;
}

template <CCowTreeConfig TConfig>
template <class TResult>
auto TCowTree<TConfig>::MutableAt(const TKey& key) -> TResult&
    requires HasValue
{
    return MutableValueHolder(key).Get();
}

template <CCowTreeConfig TConfig>
template <std::convertible_to<typename TConfig::TValue> TValueRef>
void TCowTree<TConfig>::AssignValue(const TKey& key, TValueRef&& value)
    requires HasValue
{
    YT_VERIFY(Height_ > 0);
    auto path = FindPath(key);
    YT_VERIFY(path.Exact);
    TValue replacement(std::forward<TValueRef>(value));
    MutableValueAtPath(path)->Replace(std::move(replacement));
}

template <CCowTreeConfig TConfig>
template <std::convertible_to<typename TConfig::TValue> TValueRef>
void TCowTree<TConfig>::AssignOrInsert(const TKey& key, TValueRef&& value)
    requires HasValue
{
    DoAssignOrInsert<false>(key, std::forward<TValueRef>(value), nullptr);
}

template <CCowTreeConfig TConfig>
template <bool CollectPosition, class TValueRef>
bool TCowTree<TConfig>::DoAssignOrInsert(const TKey& key, TValueRef&& value, TIterator* position)
    requires HasValue
{
    YT_VERIFY(IsMaster());

    if (COW_TREE_UNLIKELY(Height_ == 0)) {
        InsertFirst(key, std::tuple<TKey, TValue>(key, std::forward<TValueRef>(value)));
        if constexpr (CollectPosition) {
            *position = MakeIterator(static_cast<TLeaf*>(Root_.Get()), 0, nullptr, 0);
        }
        return true;
    }

    bool newGlobalMax;
    auto path = FindWritePath(key, newGlobalMax);

    if (PathHitsKey(path)) {
        // The descent that found the key also makes its leaf writable.
        TValue replacement(std::forward<TValueRef>(value));
        MutableValueAtPath(path)->Replace(std::move(replacement));
        if constexpr (CollectPosition) {
            *position = MakePathIterator(path);
        }
        return false;
    }

    InsertAtPath<CollectPosition>(path, key, std::tuple<TKey, TValue>(key, std::forward<TValueRef>(value)), newGlobalMax, position);
    return true;
}

template <CCowTreeConfig TConfig>
template <class TResult>
auto TCowTree<TConfig>::MutableOrInsert(const TKey& key) -> TResult&
    requires HasValue
{
    YT_VERIFY(IsMaster());

    if (COW_TREE_UNLIKELY(Height_ == 0)) {
        InsertFirst(key, std::tuple<TKey, TValue>(key, TValue{}));
        return static_cast<TLeaf*>(Root_.Get())->Values[0].Get();
    }

    bool newGlobalMax;
    auto path = FindWritePath(key, newGlobalMax);

    if (PathHitsKey(path)) {
        return MutableValueAtPath(path)->Get();
    }

    return InsertDefaultAtPath<TResult>(path, key, newGlobalMax);
}

template <CCowTreeConfig TConfig>
template <class TResult>
auto TCowTree<TConfig>::InsertDefaultAtPath(TPath& path, const TKey& key, bool newGlobalMax) -> TResult&
    requires HasValue
{
    // The inserted element's slot comes back through the harvested position;
    // the leaf it points into is exclusive by construction.
    TIterator position;
    InsertAtPath<true>(path, key, std::tuple<TKey, TValue>(key, TValue{}), newGlobalMax, &position);
    return position.Leaf_->Values[position.Pos_].Get();
}

template <CCowTreeConfig TConfig>
template <std::convertible_to<typename TConfig::TValue> TValueRef>
auto TCowTree<TConfig>::insert_or_assign(const TKey& key, TValueRef&& value) -> std::pair<iterator, bool>
    requires HasValue
{
    TIterator position;
    bool inserted = DoAssignOrInsert<true>(key, std::forward<TValueRef>(value), &position);
    return {iterator(std::move(position)), inserted};
}

template <CCowTreeConfig TConfig>
template <class... TArgs>
auto TCowTree<TConfig>::try_emplace(const TKey& key, TArgs&&... args) -> std::pair<iterator, bool>
    requires HasValue
{
    YT_VERIFY(IsMaster());

    if (COW_TREE_UNLIKELY(Height_ == 0)) {
        InsertFirst(key, std::tuple<TKey, TValue>(key, TValue(std::forward<TArgs>(args)...)));
        return {iterator(MakeIterator(static_cast<TLeaf*>(Root_.Get()), 0, nullptr, 0)), true};
    }

    bool newGlobalMax;
    auto path = FindWritePath(key, newGlobalMax);

    if (PathHitsKey(path)) {
        // |args| stay untouched — the std laziness guarantee — and the leaf is
        // left alone too: the returned mutable iterator copies it on write only
        // if the caller actually writes through it.
        return {iterator(MakePathIterator(path)), false};
    }

    TIterator position;
    InsertAtPath<true>(path, key, std::tuple<TKey, TValue>(key, TValue(std::forward<TArgs>(args)...)), newGlobalMax, &position);
    return {iterator(std::move(position)), true};
}

////////////////////////////////////////////////////////////////////////////////

template <CCowTreeConfig TConfig>
template <class TResult>
auto TCowTreeBase<TConfig>::at(const TKey& key) const -> const TResult&
    requires HasValue
{
    if (const auto* value = TryReadValue<TResult>(key)) {
        return *value;
    }
    throw std::out_of_range("TCowMap::at");
}

template <CCowTreeConfig TConfig>
template <class TResult>
auto TCowTree<TConfig>::at(const TKey& key) -> TResult&
    requires HasValue
{
    if (auto* holder = TryMutableValueHolder(key)) {
        return holder->Get();
    }
    throw std::out_of_range("TCowMap::at");
}

template <CCowTreeConfig TConfig>
template <class TResult>
auto TCowTree<TConfig>::operator[](const TKey& key) -> TResult&
    requires HasValue
{
    return MutableOrInsert<TResult>(key);
}

////////////////////////////////////////////////////////////////////////////////

template <CCowTreeConfig TConfig>
template <class TVisitor>
void TCowTreeBase<TConfig>::ForEach(TVisitor&& visitor) const
{
    if (Height_ > 0) {
        ForEachNode(Root_.Get(), Height_ - 1, visitor);
    }
}

template <CCowTreeConfig TConfig>
template <class TVisitor>
void TCowTreeBase<TConfig>::ForEachNode(TNode* node, int moreLevels, TVisitor& visitor)
{
    if (moreLevels == 0) {
        auto* leaf = static_cast<TLeaf*>(node);
        for (int i = 0; i < leaf->Size; ++i) {
            if constexpr (HasValue) {
                visitor(static_cast<const TKey&>(leaf->Keys[i].Get()), static_cast<const TValue&>(leaf->Values[i].Get()));
            } else {
                visitor(static_cast<const TKey&>(leaf->Keys[i].Get()));
            }
        }
        return;
    }
    auto* inner = static_cast<TInner*>(node);
    for (int i = 0; i < inner->Size; ++i) {
        ForEachNode(inner->Children[i], moreLevels - 1, visitor);
    }
}

template <CCowTreeConfig TConfig>
template <class TVisitor>
void TCowTreeBase<TConfig>::ForEachInRange(const TKey& from, const TKey& to, TVisitor&& visitor) const
{
    if (Height_ > 0 && Less_(from, to)) {
        ForEachNodeInRange(Root_.Get(), Height_ - 1, from, to, visitor);
    }
}

template <CCowTreeConfig TConfig>
template <class TVisitor>
void TCowTreeBase<TConfig>::ForEachNodeInRange(TNode* node, int moreLevels, const TKey& from, const TKey& to, TVisitor& visitor) const
{
    if (moreLevels == 0) {
        auto* leaf = static_cast<TLeaf*>(node);
        int lo = TSearch::Lookup(Less_, from, leaf->Keys, leaf->Keys + leaf->Size) - leaf->Keys;
        for (int i = lo; i < leaf->Size && Less_(leaf->Keys[i].Get(), to); ++i) {
            if constexpr (HasValue) {
                visitor(static_cast<const TKey&>(leaf->Keys[i].Get()), static_cast<const TValue&>(leaf->Values[i].Get()));
            } else {
                visitor(static_cast<const TKey&>(leaf->Keys[i].Get()));
            }
        }
        return;
    }
    auto* inner = static_cast<TInner*>(node);
    int lo = TSearch::Child(Less_, from, inner);
    int hi = TSearch::Child(Less_, to, inner);
    if (lo == hi) {
        ForEachNodeInRange(inner->Children[lo], moreLevels - 1, from, to, visitor);
        return;
    }
    // The middle children lie strictly inside the range (their subtree maxima
    // route |to| further right and |from| further left): visit them unbounded.
    ForEachNodeInRange(inner->Children[lo], moreLevels - 1, from, to, visitor);
    for (int i = lo + 1; i < hi; ++i) {
        ForEachNode(inner->Children[i], moreLevels - 1, visitor);
    }
    ForEachNodeInRange(inner->Children[hi], moreLevels - 1, from, to, visitor);
}

template <CCowTreeConfig TConfig>
void TCowTreeBase<TConfig>::AssertNoTrackedIterators(TNode* node, int moreLevels)
{
    if (moreLevels == 0) {
        YT_VERIFY(node->TrackedIterators == nullptr);
        return;
    }
    auto* inner = static_cast<TInner*>(node);
    for (int i = 0; i < inner->Size; ++i) {
        AssertNoTrackedIterators(inner->Children[i], moreLevels - 1);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::FindPath(const TKey& key) -> TPath
{
    YT_ASSERT(Height_ <= MaxHeight);
    return TSearch::template FindPath<true>(Less_, Root_.Get(), Height_, key);
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::FindInsertPath(const TKey& key) -> TPath
{
    YT_ASSERT(Height_ <= MaxHeight);
    return TSearch::template FindPath<false>(Less_, Root_.Get(), Height_, key);
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::FindRightmostPath() -> TPath
{
    YT_ASSERT(Height_ <= MaxHeight);
    return TSearch::FindRightmostPath(Root_.Get(), Height_);
}

template <CCowTreeConfig TConfig>
template <class TTuple>
bool TCowTree<TConfig>::InsertFirst(const TKey& key, TTuple&& valueTuple)
{
    YT_ASSERT(Height_ == 0);
    TNodeRef owner(NCowTreeInternals::CreateCowTreeNode<TConfig, TLeaf>());
    auto* leaf = static_cast<TLeaf*>(owner.Get());
    NCowTreeInternals::NodeDataSet(leaf, 0, std::forward<TTuple>(valueTuple));
    leaf->Size = 1;
    MaxKeyHolder_.Set(key);
    leaf->Version = ++Core_->Version;
    Root_ = std::move(owner);
    Size_ = 1;
    Height_ = 1;
    return true;
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::ResolveMaxSlot(TPath& path) -> TKeyHolder*
{
    return path.MaxSlotLevel < 0
        ? &MaxKeyHolder_
        : &path.Inner[path.MaxSlotLevel].Node->Keys[path.MaxSlotIndex];
}

template <CCowTreeConfig TConfig>
template <int PosShift, class TAction>
decltype(auto) TCowTree<TConfig>::DispatchSiblingGroup(TPathItem<TInner>& parent, TAction&& action)
{
    YT_ASSERT(parent.Node->Size >= 2);
    auto* children = parent.Node->Children + parent.Pos;
    auto* separators = parent.Node->Keys + parent.Pos;
    if (COW_TREE_UNLIKELY(parent.Node->Size == 2)) {
        if (parent.Pos == 0) {
            parent.Pos += 1 + PosShift;
            return action.template operator()<2, 0>(children, separators + 1);
        }
        parent.Pos += PosShift;
        return action.template operator()<2, 1>(children - 1, separators);
    }
    if (COW_TREE_UNLIKELY(parent.Pos == 0)) {
        parent.Pos += 2 + PosShift;
        return action.template operator()<3, 0>(children, separators + 1);
    }
    if (COW_TREE_UNLIKELY(parent.Pos == parent.Node->Size - 1)) {
        parent.Pos += PosShift;
        return action.template operator()<3, 2>(children - 2, separators - 1);
    }
    parent.Pos += 1 + PosShift;
    return action.template operator()<3, 1>(children - 1, separators);
}

////////////////////////////////////////////////////////////////////////////////

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::CopyInnerNode(TInner* source, ui64 version) -> TInner*
{
    TNodeRef owner(NCowTreeInternals::CreateCowTreeNode<TConfig, TInner>(source));
    auto* copy = static_cast<TInner*>(owner.Get());
    NCowTreeInternals::HolderCopyRange(source->Keys + 1, source->Keys + source->Size, copy->Keys + 1);
    std::memcpy(copy->Children, source->Children, source->Size * sizeof(TNode*));
    for (int i = 0; i < source->Size; ++i) {
        TNodeOps::Ref(copy->Children[i]);
    }
    copy->Size = source->Size;
    copy->Version = version;
    owner.Release();
    return copy;
}

template <CCowTreeConfig TConfig>
void TCowTree<TConfig>::AppendLeafRange(TLeaf* copy, TLeaf* source, int from, int to)
{
    int target = copy->Size;
    NCowTreeInternals::HolderCopyRange(source->Keys + from, source->Keys + to, copy->Keys + target);
    if constexpr (HasValue) {
        NCowTreeInternals::HolderCopyRange(source->Values + from, source->Values + to, copy->Values + target);
    }
    copy->Size += to - from;
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::CopyLeafNode(TLeaf* source, ui64 version, int skipPos) -> TLeaf*
{
    TNodeRef owner(NCowTreeInternals::CreateCowTreeNode<TConfig, TLeaf>());
    auto* copy = static_cast<TLeaf*>(owner.Get());
    if (skipPos < 0) {
        AppendLeafRange(copy, source, 0, source->Size);
    } else {
        AppendLeafRange(copy, source, 0, skipPos);
        AppendLeafRange(copy, source, skipPos + 1, source->Size);
    }
    copy->Version = version;
    owner.Release();
    return copy;
}

template <CCowTreeConfig TConfig>
void TCowTree<TConfig>::ReplaceInParent(TPath& path, int level, TNode* oldNode, TNode* replacement, ui64 version)
{
    if (level == 0) {
        auto* released = Root_.Release();
        YT_ASSERT(released == oldNode);
        Root_ = TNodeRef(replacement);
    } else {
        auto* parent = path.Inner[level - 1].Node;
        int slot = path.Inner[level - 1].Pos;
        YT_ASSERT(parent->Children[slot] == oldNode);
        parent->Children[slot] = replacement;
        parent->Version = version;
    }
    TNodeOps::Unref(oldNode);
}

template <CCowTreeConfig TConfig>
template <class TAnyNode>
void TCowTree<TConfig>::NormalizeGroup(TNode** groupChildren, int count, ui64 version, ui64 maxSnapshotVersion)
{
    for (int i = 0; i < count; ++i) {
        auto* node = static_cast<TAnyNode*>(groupChildren[i]);
        if (node->Version > maxSnapshotVersion) {
            continue;
        }
        TAnyNode* copy;
        if constexpr (std::is_same_v<TAnyNode, TLeaf>) {
            copy = CopyLeafNode(node, version);
            MoveLeafIterators(node, copy);
        } else {
            copy = CopyInnerNode(node, version);
            if (copy->Children[0]->IsLeaf) {
                ReparentLeafChildren(copy);
            }
        }
        groupChildren[i] = copy;
        TNodeOps::Unref(node);
    }
}

template <CCowTreeConfig TConfig>
void TCowTree<TConfig>::EnsureExclusivePath(TPath& path, ui64 version, ui64 maxSnapshotVersion)
{
    for (int level = 0; level < Height_ - 1; ++level) {
        auto* node = path.Inner[level].Node;
        if (node->Version > maxSnapshotVersion) {
            continue;
        }
        auto* copy = CopyInnerNode(node, version);
        ReplaceInParent(path, level, node, copy, version);
        path.Inner[level].Node = copy;
        if (level == Height_ - 2) {
            ReparentLeafChildren(copy);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

template <CCowTreeConfig TConfig>
bool TCowTree<TConfig>::Insert(const TKey& key)
    requires(!HasValue)
{
    return DoInsert(key, std::tuple<TKey>(key));
}

template <CCowTreeConfig TConfig>
template <std::convertible_to<typename TConfig::TValue> TValueRef>
bool TCowTree<TConfig>::Insert(const TKey& key, TValueRef&& value)
    requires HasValue
{
    // The exact-typed tuple converts the value once, right here.
    return DoInsert(key, std::tuple<TKey, TValue>(key, std::forward<TValueRef>(value)));
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::insert(const TKey& key) -> std::pair<iterator, bool>
    requires(!HasValue)
{
    TIterator position;
    bool inserted = DoInsert<true>(key, std::tuple<TKey>(key), &position);
    return {iterator(std::move(position)), inserted};
}

template <CCowTreeConfig TConfig>
template <std::convertible_to<typename TConfig::TValue> TValueRef>
auto TCowTree<TConfig>::insert(const TKey& key, TValueRef&& value) -> std::pair<iterator, bool>
    requires HasValue
{
    TIterator position;
    bool inserted = DoInsert<true>(key, std::tuple<TKey, TValue>(key, std::forward<TValueRef>(value)), &position);
    return {iterator(std::move(position)), inserted};
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::insert(const value_type& keyValue) -> std::pair<iterator, bool>
    requires HasValue
{
    return insert(keyValue.first, keyValue.second);
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::insert(value_type&& keyValue) -> std::pair<iterator, bool>
    requires HasValue
{
    return insert(keyValue.first, std::move(keyValue.second));
}

template <CCowTreeConfig TConfig>
template <class... TArgs>
auto TCowTree<TConfig>::emplace(TArgs&&... args) -> std::pair<iterator, bool>
    requires(!HasValue)
{
    return insert(TKey(std::forward<TArgs>(args)...));
}

template <CCowTreeConfig TConfig>
template <class TKeyArg, class TValueArg>
auto TCowTree<TConfig>::emplace(TKeyArg&& key, TValueArg&& value) -> std::pair<iterator, bool>
    requires HasValue
{
    return insert(TKey(std::forward<TKeyArg>(key)), std::forward<TValueArg>(value));
}

template <CCowTreeConfig TConfig>
template <class TTuple>
void TCowTree<TConfig>::DoInsertHint(TIterator& position, const TKey& key, TTuple&& valueTuple)
{
    YT_VERIFY(IsMaster());
    YT_VERIFY(position.Tree_ == this);

    auto* leaf = position.Leaf_;
    ui64 maxSnapshotVersion = Core_->GetMaxLiveSnapshotVersion();
    // The fast path mirrors EraseLeafSlot: an exclusive leaf (exclusivity is
    // upward-closed, so one check clears the whole path) with a free slot, whose
    // key range strictly encloses |key| — then this leaf is the only place the
    // key may live, and neither the separators nor the tree max can change.
    if (COW_TREE_LIKELY(
        leaf != nullptr &&
        leaf->Version > maxSnapshotVersion &&
        leaf->Size < MaxNodeSize &&
        Less_(leaf->Keys[0].Get(), key) &&
        Less_(key, leaf->Keys[leaf->Size - 1].Get())))
    {
        int pos = TSearch::LookupForWrite(Less_, key, leaf->Keys, leaf->Keys + leaf->Size) - leaf->Keys;
        if (COW_TREE_UNLIKELY(!Less_(key, leaf->Keys[pos].Get()))) {
            // Already present: just repark the iterator on it.
            position.Pos_ = pos;
            return;
        }
        ui64 version = ++Core_->Version;
        NCowTreeInternals::NodeInsertSimple(leaf, pos, std::forward<TTuple>(valueTuple));
        leaf->Version = version;
        Size_++;
        FixLeafInsert(leaf, pos);
        position.Pos_ = pos;
        return;
    }

    // A boundary or wrong hint: one plain insert descent, which also parks the
    // iterator on the (inserted or preexisting) element.
    DoInsert<true>(key, std::forward<TTuple>(valueTuple), &position);
}

template <CCowTreeConfig TConfig>
void TCowTree<TConfig>::InsertHint(TIterator& position, const TKey& key)
    requires(!HasValue)
{
    DoInsertHint(position, key, std::tuple<TKey>(key));
}

template <CCowTreeConfig TConfig>
template <std::convertible_to<typename TConfig::TValue> TValueRef>
void TCowTree<TConfig>::InsertHint(TIterator& position, const TKey& key, TValueRef&& value)
    requires HasValue
{
    DoInsertHint(position, key, std::tuple<TKey, TValue>(key, std::forward<TValueRef>(value)));
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::insert(TIterator hint, const TKey& key) -> iterator
    requires(!HasValue)
{
    InsertHint(hint, key);
    return iterator(std::move(hint));
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::insert(TIterator hint, const value_type& keyValue) -> iterator
    requires HasValue
{
    InsertHint(hint, keyValue.first, keyValue.second);
    return iterator(std::move(hint));
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::insert(TIterator hint, value_type&& keyValue) -> iterator
    requires HasValue
{
    InsertHint(hint, keyValue.first, std::move(keyValue.second));
    return iterator(std::move(hint));
}

template <CCowTreeConfig TConfig>
template <class... TArgs>
auto TCowTree<TConfig>::emplace_hint(TIterator hint, TArgs&&... args) -> iterator
    requires(!HasValue)
{
    InsertHint(hint, TKey(std::forward<TArgs>(args)...));
    return iterator(std::move(hint));
}

template <CCowTreeConfig TConfig>
template <class TKeyArg, class TValueArg>
auto TCowTree<TConfig>::emplace_hint(TIterator hint, TKeyArg&& key, TValueArg&& value) -> iterator
    requires HasValue
{
    InsertHint(hint, TKey(std::forward<TKeyArg>(key)), std::forward<TValueArg>(value));
    return iterator(std::move(hint));
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::FindWritePath(const TKey& key, bool& newGlobalMax) -> TPath
{
    YT_ASSERT(Height_ > 0);
    newGlobalMax = Less_(MaxKeyHolder_.Get(), key);
    return newGlobalMax ? FindRightmostPath() : FindInsertPath(key);
}

template <CCowTreeConfig TConfig>
bool TCowTree<TConfig>::PathHitsKey(const TPath& path) const
{
    return path.Exact;
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::MakePathIterator(const TPath& path) -> TIterator
{
    if (COW_TREE_LIKELY(Height_ > 1)) {
        return MakeIterator(path.Leaf.Node, path.Leaf.Pos, path.Inner[Height_ - 2].Node, path.Inner[Height_ - 2].Pos);
    }
    return MakeIterator(path.Leaf.Node, path.Leaf.Pos, nullptr, 0);
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::MutableValueAtPath(TPath& path) -> NCowTreeInternals::TValueHolder<TValue>*
    requires HasValue
{
    ui64 maxSnapshotVersion = Core_->GetMaxLiveSnapshotVersion();
    ui64 version = ++Core_->Version;
    auto* leaf = path.Leaf.Node;
    if (COW_TREE_UNLIKELY(leaf->Version <= maxSnapshotVersion)) {
        EnsureExclusivePath(path, version, maxSnapshotVersion);
        auto* copy = CopyLeafNode(leaf, version);
        MoveLeafIterators(leaf, copy);
        ReplaceInParent(path, Height_ - 1, leaf, copy, version);
        leaf = copy;
        path.Leaf.Node = copy;
    } else {
        leaf->Version = version;
    }
    return &leaf->Values[path.Leaf.Pos];
}

// Inlining keeps the caller's descent path in registers.
template <CCowTreeConfig TConfig>
template <bool CollectPosition, class TTuple>
void TCowTree<TConfig>::InsertAtPath(TPath& path, const TKey& key, TTuple&& valueTuple, bool newGlobalMax, TIterator* position)
{
    ui64 maxSnapshotVersion = Core_->GetMaxLiveSnapshotVersion();
    ui64 version = ++Core_->Version;

    std::optional<TKey> nextMax;
    if (COW_TREE_UNLIKELY(newGlobalMax)) {
        nextMax.emplace(key);
    }

    bool leafShared = path.Leaf.Node->Version <= maxSnapshotVersion;
    if (COW_TREE_UNLIKELY(leafShared)) {
        EnsureExclusivePath(path, version, maxSnapshotVersion);
    }

    // Once the path is exclusive, the parent is stable and its slot is only a hint.
    struct TRawPosition
    {
        TLeaf* Leaf = nullptr;
        int Pos = 0;
        TInner* Parent = nullptr;
        int ParentPos = 0;
    };

    [[maybe_unused]] TRawPosition raw;
    if constexpr (CollectPosition) {
        if (Height_ > 1) {
            raw.Parent = path.Inner[Height_ - 2].Node;
            raw.ParentPos = path.Inner[Height_ - 2].Pos;
        }
    }

    auto insertInLevel = [&]<class TAnyNode, class TLevelTuple>(int level, const TPathItem<TAnyNode>& pathItem, TLevelTuple&& value) {
        constexpr bool IsLeafLevel = !NCowTreeInternals::NodeHasChildren<TAnyNode>;
        // Whether the group members of this level are the direct parents of leaves,
        // so that moving their children around moves the iterators' parent hints.
        [[maybe_unused]] bool leafParentLevel = !IsLeafLevel && level == Height_ - 2;
        if (COW_TREE_LIKELY(pathItem.Node->Size < MaxNodeSize)) {
            NodeInsertSimple(pathItem.Node, pathItem.Pos, std::forward<TLevelTuple>(value));
            if constexpr (IsLeafLevel) {
                FixLeafInsert(pathItem.Node, pathItem.Pos);
                if constexpr (CollectPosition) {
                    raw.Leaf = pathItem.Node;
                    raw.Pos = pathItem.Pos;
                }
            }
            pathItem.Node->Version = version;
            return std::optional<std::tuple<TKey, TNode*>>();
        } else if (COW_TREE_UNLIKELY(level == 0)) {
            // TPath holds at most MaxHeight levels; growing beyond that would walk past
            // path.Inner[] on the next modification, so fail loudly instead.
            YT_VERIFY(Height_ < MaxHeight);
            auto* newNode = NCowTreeInternals::CreateCowTreeNode<TConfig, TAnyNode>(pathItem.Node);
            auto* newRoot = NCowTreeInternals::CreateCowTreeNode<TConfig, TInner>();
            NCowTreeInternals::TValueHolder<TKey> upSeparator;
            TNode* rootNode[1] = {Root_.Get()};
            TIterator* chain = nullptr;
            if constexpr (IsLeafLevel) {
                chain = CollectGroupIterators(rootNode, 1, pathItem.Pos, +1);
            }
            NCowTreeInternals::NodeInsertExpand<1, 0, TAnyNode>(rootNode, static_cast<NCowTreeInternals::TValueHolder<TKey>*>(nullptr), newNode, upSeparator, pathItem.Pos, std::forward<TLevelTuple>(value), MaxNodeSize + 1);
            pathItem.Node->Version = version;
            newNode->Version = version;
            if constexpr (IsLeafLevel) {
                TNode* halves[2] = {pathItem.Node, newNode};
                DistributeGroupIterators(chain, halves, 2);
            }
            newRoot->Keys[1].Set(std::move(upSeparator.Get()));
            upSeparator.Clear();
            newRoot->Children[0] = pathItem.Node;
            newRoot->Children[1] = newNode;
            newRoot->Size = 2;
            newRoot->Version = version;
            // The old root's reference unit moves into the new root's child slot.
            Root_.Release();
            Root_ = TNodeRef(newRoot);
            if constexpr (IsLeafLevel) {
                ReparentLeafChildren(newRoot);
                if constexpr (CollectPosition) {
                    int leftSize = pathItem.Node->Size;
                    if (pathItem.Pos < leftSize) {
                        raw = {pathItem.Node, pathItem.Pos, newRoot, 0};
                    } else {
                        raw = {static_cast<TLeaf*>(newNode), pathItem.Pos - leftSize, newRoot, 1};
                    }
                }
            } else if (leafParentLevel) {
                ReparentLeafChildren(static_cast<TInner*>(pathItem.Node));
                ReparentLeafChildren(static_cast<TInner*>(newNode));
            }
            Height_++;
            return std::optional<std::tuple<TKey, TNode*>>();
        } else {
            auto runComplex = [&]<size_t NumNodes, size_t InsertIdx>(TNode** groupChildren, NCowTreeInternals::TValueHolder<TKey>* groupSeparators) {
                NormalizeGroup<TAnyNode>(groupChildren, NumNodes, version, maxSnapshotVersion);
                TIterator* chain = nullptr;
                [[maybe_unused]] int insertStreamPos = 0;
                if constexpr (IsLeafLevel) {
                    insertStreamPos = pathItem.Pos;
                    for (int i = 0; i < static_cast<int>(InsertIdx); ++i) {
                        insertStreamPos += groupChildren[i]->Size;
                    }
                    chain = CollectGroupIterators(groupChildren, NumNodes, insertStreamPos, +1);
                }
                auto result = NodeInsertComplex<NumNodes, InsertIdx, TAnyNode>(groupChildren, groupSeparators, pathItem.Pos, std::forward<TLevelTuple>(value));
                for (int i = 0; i < static_cast<int>(NumNodes); ++i) {
                    groupChildren[i]->Version = version;
                }
                if (result) {
                    std::get<1>(*result)->Version = version;
                }
                if constexpr (IsLeafLevel) {
                    TNode* targets[NumNodes + 1];
                    std::copy(groupChildren, groupChildren + NumNodes, targets);
                    int targetCount = NumNodes;
                    if (result) {
                        targets[targetCount++] = std::get<1>(*result);
                    }
                    DistributeGroupIterators(chain, targets, targetCount);
                    if constexpr (CollectPosition) {
                        int remaining = insertStreamPos;
                        for (int i = 0; i < targetCount; ++i) {
                            if (remaining < targets[i]->Size) {
                                raw.Leaf = static_cast<TLeaf*>(targets[i]);
                                raw.Pos = remaining;
                                break;
                            }
                            remaining -= targets[i]->Size;
                        }
                    }
                } else if (leafParentLevel) {
                    for (int i = 0; i < static_cast<int>(NumNodes); ++i) {
                        ReparentLeafChildren(static_cast<TInner*>(groupChildren[i]));
                    }
                    if (result) {
                        ReparentLeafChildren(static_cast<TInner*>(std::get<1>(*result)));
                    }
                }
                path.Inner[level - 1].Node->Version = version;
                return result;
            };
            return DispatchSiblingGroup<1>(path.Inner[level - 1], runComplex);
        }
    };

    int level = Height_ - 1;
    auto insertResult = [&] () -> std::optional<std::tuple<TKey, TNode*>> {
        if (COW_TREE_UNLIKELY(leafShared) && COW_TREE_LIKELY(path.Leaf.Node->Size < MaxNodeSize)) {
            // The frequent shared-leaf case: one fused copy+insert pass.
            auto* source = path.Leaf.Node;
            TNodeRef owner(NCowTreeInternals::CreateCowTreeNode<TConfig, TLeaf>());
            auto* copy = static_cast<TLeaf*>(owner.Get());
            copy->Version = version;
            AppendLeafRange(copy, source, 0, path.Leaf.Pos);
            NCowTreeInternals::NodeDataSet(copy, path.Leaf.Pos, std::forward<TTuple>(valueTuple));
            ++copy->Size;
            AppendLeafRange(copy, source, path.Leaf.Pos, source->Size);
            owner.Release();
            MoveLeafIterators(source, copy, /*insertPos*/ path.Leaf.Pos);
            ReplaceInParent(path, Height_ - 1, source, copy, version);
            path.Leaf.Node = copy;
            if constexpr (CollectPosition) {
                raw.Leaf = copy;
                raw.Pos = path.Leaf.Pos;
            }
            return std::nullopt;
        }
        if (COW_TREE_UNLIKELY(leafShared)) {
            // A full shared leaf: normalize it and let the split machinery run in place.
            auto* source = path.Leaf.Node;
            auto* copy = CopyLeafNode(source, version);
            MoveLeafIterators(source, copy);
            ReplaceInParent(path, Height_ - 1, source, copy, version);
            path.Leaf.Node = copy;
        }
        return insertInLevel(level, path.Leaf, std::forward<TTuple>(valueTuple));
    }();
    if constexpr (CollectPosition) {
        // Registered before the cascade continues upward: parent-level splits
        // repair this iterator like any other live one.
        *position = MakeIterator(raw.Leaf, raw.Pos, raw.Parent, raw.ParentPos);
    }
    for (level--; insertResult; level--) {
        auto split = insertInLevel(level, path.Inner[level], std::move(*insertResult));
        if (!split) {
            break;
        }
        insertResult.emplace(std::move(*split));
    }
    if (COW_TREE_UNLIKELY(newGlobalMax)) {
        MaxKeyHolder_.Clear();
        MaxKeyHolder_.Set(std::move(*nextMax));
    }
    ++Size_;
}

template <CCowTreeConfig TConfig>
template <bool CollectPosition, class TTuple>
bool TCowTree<TConfig>::DoInsert(const TKey& key, TTuple&& valueTuple, TIterator* position)
{
    YT_VERIFY(IsMaster());

    if (COW_TREE_UNLIKELY(Height_ == 0)) {
        bool inserted = InsertFirst(key, std::forward<TTuple>(valueTuple));
        if constexpr (CollectPosition) {
            *position = MakeIterator(static_cast<TLeaf*>(Root_.Get()), 0, nullptr, 0);
        }
        return inserted;
    }

    bool newGlobalMax;
    auto path = FindWritePath(key, newGlobalMax);

    if (COW_TREE_UNLIKELY(PathHitsKey(path))) {
        if constexpr (CollectPosition) {
            *position = MakePathIterator(path);
        }
        return false;
    }

    InsertAtPath<CollectPosition>(path, key, std::forward<TTuple>(valueTuple), newGlobalMax, position);
    return true;
}

////////////////////////////////////////////////////////////////////////////////

template <CCowTreeConfig TConfig>
bool TCowTree<TConfig>::EraseLast()
{
    YT_ASSERT(Height_ == 1);
    YT_ASSERT(Size_ == 0);
    auto* leaf = static_cast<TLeaf*>(Root_.Get());
    YT_ASSERT(leaf->Size == 1);
    // The last element goes away: its iterators become end().
    EvictLeafIterators(leaf, 0);
    Root_.Reset();
    Height_ = 0;
    MaxKeyHolder_.Clear();
    return true;
}

template <CCowTreeConfig TConfig>
size_t TCowTree<TConfig>::erase(const TKey& key)
{
    YT_VERIFY(IsMaster());

    if (COW_TREE_UNLIKELY(Height_ == 0)) {
        return false;
    }

    // An exact match against the cached maximum takes the rightmost path.
    const bool atMax = !Less_(key, MaxKeyHolder_.Get());
    if (COW_TREE_UNLIKELY(atMax) && Less_(MaxKeyHolder_.Get(), key)) {
        return false;
    }
    auto path = COW_TREE_UNLIKELY(atMax) ? FindRightmostPath() : FindPath(key);
    if (COW_TREE_UNLIKELY(atMax)) {
        path.Leaf.Pos = path.Leaf.NodeSize() - 1;
        path.Exact = true;
    }

    if (COW_TREE_UNLIKELY(!path.Exact)) {
        return false;
    }

    ui64 maxSnapshotVersion = Core_->GetMaxLiveSnapshotVersion();
    ui64 version = ++Core_->Version;
    if (COW_TREE_UNLIKELY(Size_ == 1)) {
        --Size_;
        return EraseLast();
    }

    bool leafShared = path.Leaf.Node->Version <= maxSnapshotVersion;
    if (COW_TREE_UNLIKELY(leafShared)) {
        EnsureExclusivePath(path, version, maxSnapshotVersion);
    }

    // Erasing a leaf's max leaves its one extra copy (the MaxSlot separator) stale. The
    // slot is repaired right after the leaf-level op — the ancestors are restructured
    // later, on the way up — unless the op's own separator range covered it (then the
    // machinery or the pending pair-erase repairs it).
    bool eraseMax = path.Leaf.Pos == path.Leaf.NodeSize() - 1;
    if (COW_TREE_UNLIKELY(eraseMax && LeafIterators(path.Leaf.Node))) {
        AdvanceLastLeafIterators(path);
    }

    TLeaf* lastLeaf = nullptr;
    TInner* separatorsNode = nullptr;
    int separatorsBegin = 0;
    int separatorsEnd = 0;

    auto eraseInLevel = [&]<class TAnyNode>(int level, const TPathItem<TAnyNode>& pathItem) {
        constexpr bool IsLeafLevel = std::is_same_v<TAnyNode, TLeaf>;
        // Whether the group members of this level are the direct parents of leaves,
        // so that moving their children around moves the iterators' parent hints.
        [[maybe_unused]] bool leafParentLevel = !IsLeafLevel && level == Height_ - 2;
        bool cascade = false;
        if constexpr (IsLeafLevel) {
            lastLeaf = pathItem.Node;
        }
        bool simple = pathItem.Node->Size > MaxNodeSize * 2 / 3;
        if constexpr (IsLeafLevel) {
            // The leaf level adds the group-average underfill policy; a root leaf
            // falls through to the level == 0 branch.
            if (COW_TREE_UNLIKELY(!simple) && level > 0) {
                simple = LeafEraseKeepsFill(pathItem.Node, path.Inner[level - 1].Node, path.Inner[level - 1].Pos);
            }
        }
        if (COW_TREE_LIKELY(simple)) {
            NodeEraseSimple<IsLeafLevel>(pathItem.Node, pathItem.Pos);
            if constexpr (IsLeafLevel) {
                FixLeafErase(pathItem.Node, pathItem.Pos);
            }
            pathItem.Node->Version = version;
        } else if (COW_TREE_UNLIKELY(level == 0)) {
            YT_ASSERT(pathItem.Node->Size > 1);
            NodeEraseSimple<IsLeafLevel>(pathItem.Node, pathItem.Pos);
            if constexpr (IsLeafLevel) {
                FixLeafErase(pathItem.Node, pathItem.Pos);
            }
            pathItem.Node->Version = version;
            if constexpr (std::is_same_v<TAnyNode, TInner>) {
                if (pathItem.Node->Size == 1) {
                    // Detach the sole child before releasing the old root, so the
                    // release does not cascade into it.
                    auto* oldRoot = pathItem.Node;
                    TNodeRef newRoot(oldRoot->Children[0]);
                    oldRoot->Size = 0;
                    Root_ = std::move(newRoot);
                    Height_--;
                    if (Height_ == 1) {
                        SetLeafParent(static_cast<TLeaf*>(Root_.Get()), nullptr, 0);
                    }
                }
            }
        } else {
            auto runComplex = [&]<size_t NumNodes, size_t EraseIdx>(TNode** groupChildren, NCowTreeInternals::TValueHolder<TKey>* groupSeparators) {
                NormalizeGroup<TAnyNode>(groupChildren, NumNodes, version, maxSnapshotVersion);
                TIterator* chain = nullptr;
                if constexpr (IsLeafLevel) {
                    int eraseStreamPos = pathItem.Pos;
                    for (int i = 0; i < static_cast<int>(EraseIdx); ++i) {
                        eraseStreamPos += groupChildren[i]->Size;
                    }
                    chain = CollectGroupIterators(groupChildren, NumNodes, eraseStreamPos + 1, -1);
                }
                cascade = NodeEraseComplex<IsLeafLevel, NumNodes, EraseIdx, TAnyNode>(groupChildren, groupSeparators, pathItem.Pos);
                for (int i = 0; i < static_cast<int>(NumNodes) - (cascade ? 1 : 0); ++i) {
                    groupChildren[i]->Version = version;
                }
                path.Inner[level - 1].Node->Version = version;
                if constexpr (IsLeafLevel) {
                    DistributeGroupIterators(chain, groupChildren, NumNodes - (cascade ? 1 : 0));
                    lastLeaf = static_cast<TLeaf*>(groupChildren[NumNodes - 1 - (cascade ? 1 : 0)]);
                    separatorsNode = path.Inner[level - 1].Node;
                    separatorsBegin = groupSeparators - separatorsNode->Keys;
                    separatorsEnd = separatorsBegin + NumNodes - 1;
                } else if (leafParentLevel) {
                    for (int i = 0; i < static_cast<int>(NumNodes) - (cascade ? 1 : 0); ++i) {
                        ReparentLeafChildren(static_cast<TInner*>(groupChildren[i]));
                    }
                }
            };
            DispatchSiblingGroup<0>(path.Inner[level - 1], runComplex);
        }
        return cascade;
    };

    int level = Height_ - 1;
    bool eraseResult;
    if (COW_TREE_UNLIKELY(leafShared) &&
        COW_TREE_LIKELY(path.Leaf.Node->Size > MaxNodeSize * 2 / 3 || Height_ == 1 ||
            LeafEraseKeepsFill(path.Leaf.Node, path.Inner[Height_ - 2].Node, path.Inner[Height_ - 2].Pos)))
    {
        // The frequent shared-leaf case: one fused copy+erase pass.
        auto* source = path.Leaf.Node;
        TNodeRef owner(CopyLeafNode(source, version, path.Leaf.Pos));
        auto* copy = static_cast<TLeaf*>(owner.Release());
        MoveLeafIterators(source, copy, /*insertPos*/ -1, /*skipPos*/ path.Leaf.Pos);
        ReplaceInParent(path, Height_ - 1, source, copy, version);
        path.Leaf.Node = copy;
        lastLeaf = copy;
        eraseResult = false;
    } else {
        if (COW_TREE_UNLIKELY(leafShared)) {
            // An underfull shared leaf: normalize it and let the merge machinery run.
            auto* source = path.Leaf.Node;
            auto* copy = CopyLeafNode(source, version);
            MoveLeafIterators(source, copy);
            ReplaceInParent(path, Height_ - 1, source, copy, version);
            path.Leaf.Node = copy;
        }
        eraseResult = eraseInLevel(level, path.Leaf);
    }
    if (COW_TREE_UNLIKELY(eraseMax)) {
        RepairMaxSeparator(path, lastLeaf, separatorsNode, separatorsBegin, separatorsEnd, version);
    }
    for (level--; eraseResult; level--) {
        eraseResult = eraseInLevel(level, path.Inner[level]);
    }
    --Size_;
    return true;
}

template <CCowTreeConfig TConfig>
int TCowTree<TConfig>::RepairLeafSlot(TInner* parent, TLeaf* leaf, int hint)
{
    if (COW_TREE_LIKELY(hint < parent->Size && parent->Children[hint] == leaf)) {
        return hint;
    }
    for (int i = 0;; ++i) {
        YT_ASSERT(i < parent->Size);
        if (parent->Children[i] == leaf) {
            return i;
        }
    }
}

template <CCowTreeConfig TConfig>
void TCowTree<TConfig>::EraseLeafSlot(TLeaf* leaf, int pos, TInner* parent, int parentPos)
{
    ui64 maxSnapshotVersion = Core_->GetMaxLiveSnapshotVersion();
    // Everything below runs off the iterator's parent hint, with no root descent.
    // It needs an exclusive leaf — exclusivity is upward-closed, so this one check
    // clears the whole path — and, when the erase takes the leaf's max, a parent
    // that still holds the extra copy of it.
    if (COW_TREE_LIKELY(leaf->Version > maxSnapshotVersion)) {
        int leafSlot = -1;
        bool fillOk;
        if (COW_TREE_LIKELY(leaf->Size > MaxNodeSize * 2 / 3 || Height_ == 1)) {
            fillOk = true;
        } else if (leaf->Size > MaxNodeSize / 2) {
            leafSlot = RepairLeafSlot(parent, leaf, parentPos);
            fillOk = LeafEraseKeepsFill(leaf, parent, leafSlot);
        } else {
            fillOk = false;
        }

        // Erasing the leaf's max leaves its one extra copy — the separator right
        // behind the leaf in its parent — stale. That copy is reachable through
        // the hint as long as the leaf is not its parent's last child; a rightmost
        // leaf keeps the copy further up, out of the hint's reach. Writing the
        // parent needs no path either: an exclusive leaf implies an exclusive parent.
        bool maxErase = pos == leaf->Size - 1;
        int separatorSlot = -1;
        if (COW_TREE_UNLIKELY(maxErase) && Height_ > 1) {
            if (leafSlot < 0) {
                leafSlot = RepairLeafSlot(parent, leaf, parentPos);
            }
            if (leafSlot + 1 < parent->Size) {
                separatorSlot = leafSlot + 1;
            }
        }
        bool maxReachable = !maxErase || separatorSlot >= 0;

        if (COW_TREE_LIKELY(fillOk && maxReachable)) {
            ui64 version = ++Core_->Version;
            // Iterators standing on the leaf's max have no successor inside the
            // leaf; they move to the head of the parent's next child.
            TIterator* strand = COW_TREE_UNLIKELY(separatorSlot >= 0)
                ? DetachIteratorsAt(leaf, pos)
                : nullptr;
            NCowTreeInternals::NodeEraseSimple<true>(leaf, pos);
            leaf->Version = version;
            Size_--;
            // Elsewhere the successor slides into the erased position, so the
            // leaf's live iterators follow it with no root descent anywhere.
            FixLeafErase(leaf, pos);
            if (COW_TREE_UNLIKELY(separatorSlot >= 0)) {
                auto& separator = parent->Keys[separatorSlot];
                separator.Clear();
                separator.CopyFrom(leaf->Keys[leaf->Size - 1]);
                parent->Version = version;
                if (strand) {
                    ReseatStrandToNextLeaf(strand, parent, separatorSlot);
                }
            }
            return;
        }

        // An underfull leaf needs no descent either: its sibling group is exactly
        // the parent's children. The one thing the hint cannot reach is a cascade
        // above the parent, so this runs only while the parent can absorb the
        // shrink with a simple erase of its own (the root absorbs any).
        if (maxReachable && Height_ > 1 &&
            (parent->Size > MaxNodeSize * 2 / 3 || parent == Root_.Get()))
        {
            if (leafSlot < 0) {
                leafSlot = RepairLeafSlot(parent, leaf, parentPos);
            }
            EraseLeafGroup(pos, parent, leafSlot, separatorSlot);
            return;
        }
    }

    // What is left — a leaf shared with a snapshot, a max copy living above the
    // parent, a cascade past the parent — reuses the by-key machinery, which
    // repairs the live iterators (the caller's among them) like any mutation does.
    TKey key = leaf->Keys[pos].Get();
    erase(key);
}

template <CCowTreeConfig TConfig>
void TCowTree<TConfig>::EraseLeafGroup(int pos, TInner* parent, int leafSlot, int separatorSlot)
{
    ui64 maxSnapshotVersion = Core_->GetMaxLiveSnapshotVersion();
    ui64 version = ++Core_->Version;
    TPathItem<TInner> parentItem{parent, leafSlot};
    bool cascade = false;
    TLeaf* lastLeaf = nullptr;
    int separatorsBegin = 0;
    int separatorsEnd = 0;

    TIterator* strand = nullptr;
    auto runComplex = [&]<size_t NumNodes, size_t EraseIdx>(TNode** groupChildren, TKeyHolder* groupSeparators) {
        NormalizeGroup<TLeaf>(groupChildren, NumNodes, version, maxSnapshotVersion);
        if constexpr (EraseIdx + 1 == NumNodes) {
            // The erased element is the max of the group's last node, so it has no
            // successor inside the group: park those iterators for the reseat below.
            if (COW_TREE_UNLIKELY(separatorSlot >= 0)) {
                strand = DetachIteratorsAt(static_cast<TLeaf*>(groupChildren[EraseIdx]), pos);
            }
        }
        int eraseStreamPos = pos;
        for (int i = 0; i < static_cast<int>(EraseIdx); ++i) {
            eraseStreamPos += groupChildren[i]->Size;
        }
        auto* chain = CollectGroupIterators(groupChildren, NumNodes, eraseStreamPos + 1, -1);
        cascade = NCowTreeInternals::NodeEraseComplex<true, NumNodes, EraseIdx, TLeaf>(groupChildren, groupSeparators, pos);
        int survivors = static_cast<int>(NumNodes) - (cascade ? 1 : 0);
        for (int i = 0; i < survivors; ++i) {
            groupChildren[i]->Version = version;
        }
        DistributeGroupIterators(chain, groupChildren, survivors);
        lastLeaf = static_cast<TLeaf*>(groupChildren[survivors - 1]);
        separatorsBegin = groupSeparators - parent->Keys;
        separatorsEnd = separatorsBegin + NumNodes - 1;
    };
    DispatchSiblingGroup<0>(parentItem, runComplex);
    parent->Version = version;

    // The group rewrote its own separators; the max copy needs a hand only when it
    // sits outside that range.
    if (COW_TREE_UNLIKELY(separatorSlot >= 0)) {
        if (separatorSlot < separatorsBegin || separatorSlot >= separatorsEnd) {
            auto* maxSlot = &parent->Keys[separatorSlot];
            maxSlot->Clear();
            maxSlot->CopyFrom(lastLeaf->Keys[lastLeaf->Size - 1]);
        }
    }

    if (COW_TREE_UNLIKELY(cascade)) {
        // The group shrank: erase the (already emptied key, dropped child) pair
        // from the parent. The caller's precondition makes this a simple erase.
        NCowTreeInternals::NodeEraseSimple<false>(parent, parentItem.Pos);
        if (COW_TREE_UNLIKELY(strand != nullptr)) {
            // The dropped pair shifted the successor's child slot down by one.
            ReseatStrandToNextLeaf(strand, parent, parentItem.Pos);
            strand = nullptr;
        }
        if (COW_TREE_UNLIKELY(parent->Size == 1)) {
            // The root lost its last separator: its sole child takes over.
            YT_ASSERT(parent == Root_.Get());
            TNodeRef newRoot(parent->Children[0]);
            parent->Size = 0;
            Root_ = std::move(newRoot);
            Height_--;
            if (Height_ == 1) {
                SetLeafParent(static_cast<TLeaf*>(Root_.Get()), nullptr, 0);
            }
        }
    }
    if (COW_TREE_UNLIKELY(strand != nullptr)) {
        ReseatStrandToNextLeaf(strand, parent, parentItem.Pos + 1);
    }
    --Size_;
}

template <CCowTreeConfig TConfig>
void TCowTree<TConfig>::EraseAdvance(TIterator& position)
{
    YT_VERIFY(IsMaster());
    YT_VERIFY(position.Tree_ == this);
    YT_VERIFY(position.Leaf_ != nullptr);
    EraseLeafSlot(position.Leaf_, position.Pos_, position.Parent_, position.ParentPos_);
}

template <CCowTreeConfig TConfig>
void TCowTree<TConfig>::EraseAt(TIterator& position)
{
    YT_VERIFY(IsMaster());
    YT_VERIFY(position.Tree_ == this);
    YT_VERIFY(position.Leaf_ != nullptr);
    auto* leaf = position.Leaf_;
    int pos = position.Pos_;
    auto* parent = position.Parent_;
    int parentPos = position.ParentPos_;
    // The caller is done with the iterator: unregister it up front, so the erase
    // runs with one tracked iterator fewer and never repairs this one.
    position.ResetState();
    EraseLeafSlot(leaf, pos, parent, parentPos);
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::erase(TIterator position) -> iterator
{
    EraseAdvance(position);
    return iterator(std::move(position));
}

template <CCowTreeConfig TConfig>
bool TCowTree<TConfig>::LeafEraseKeepsFill(TLeaf* leaf, TInner* parent, int parentPos) const
{
    if (leaf->Size <= MaxNodeSize / 2) {
        return false;
    }
    int groupSize = std::min(parent->Size, 3);
    int firstChild = std::min(std::max(parentPos - 1, 0), parent->Size - groupSize);
    int groupTotal = 0;
    for (int i = 0; i < groupSize; ++i) {
        groupTotal += parent->Children[firstChild + i]->Size;
    }
    return 3 * groupTotal > 2 * MaxNodeSize * groupSize;
}

template <CCowTreeConfig TConfig>
void TCowTree<TConfig>::RepairMaxSeparator(TPath& path, TLeaf* lastLeaf, TInner* separatorsNode, int separatorsBegin, int separatorsEnd, ui64 version)
{
    // The group op has just rewritten the separators of |separatorsNode| in the slot
    // range [begin, end); when the max copy is one of them, it is already up to date.
    // Ownership is decided by node identity: the slot may equally live in another
    // node or in MaxKeyHolder_, whose addresses are unordered with respect to these.
    if (separatorsNode &&
        path.MaxSlotLevel >= 0 &&
        path.Inner[path.MaxSlotLevel].Node == separatorsNode &&
        path.MaxSlotIndex >= separatorsBegin &&
        path.MaxSlotIndex < separatorsEnd)
    {
        return;
    }
    auto* maxSlot = ResolveMaxSlot(path);
    maxSlot->Clear();
    maxSlot->CopyFrom(lastLeaf->Keys[lastLeaf->Size - 1]);
    if (path.MaxSlotLevel >= 0) {
        path.Inner[path.MaxSlotLevel].Node->Version = version;
    }
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::ExtractAt(TIterator& position) -> TKey
    requires(!HasValue)
{
    YT_VERIFY(position.Leaf_ != nullptr);
    TKey extracted = position.Leaf_->Keys[position.Pos_].Get();
    EraseAt(position);
    return extracted;
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::ExtractAt(TIterator& position) -> value_type
    requires HasValue
{
    YT_VERIFY(position.Leaf_ != nullptr);
    // Moving the value out is a modification, so it needs an exclusive leaf of its
    // own: on a leaf shared with a snapshot the move would be seen through it. The
    // copy-on-write access repoints |position| to the private leaf; the key is read
    // afterwards and copied, as the erase below may take the by-key fallback.
    auto& value = MutableValueFor(position);
    TKey key(position.Leaf_->Keys[position.Pos_].Get());
    auto& slot = position.Leaf_->Values[position.Pos_];
    TValue extracted(std::move(value));
    try {
        EraseAt(position);
    } catch (...) {
        // A throwing lookup leaves the original slot in place.
        slot.Replace(std::move(extracted));
        throw;
    }
    return {std::move(key), std::move(extracted)};
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::ExtractAdvance(TIterator& position) -> TKey
    requires(!HasValue)
{
    YT_VERIFY(position.Leaf_ != nullptr);
    TKey extracted = position.Leaf_->Keys[position.Pos_].Get();
    EraseAdvance(position);
    return extracted;
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::ExtractAdvance(TIterator& position) -> value_type
    requires HasValue
{
    YT_VERIFY(position.Leaf_ != nullptr);
    // See the copy-on-write note in #ExtractAt().
    auto& value = MutableValueFor(position);
    TKey key(position.Leaf_->Keys[position.Pos_].Get());
    auto& slot = position.Leaf_->Values[position.Pos_];
    TValue extracted(std::move(value));
    try {
        EraseAdvance(position);
    } catch (...) {
        // A throwing lookup leaves the original slot in place.
        slot.Replace(std::move(extracted));
        throw;
    }
    return {std::move(key), std::move(extracted)};
}

template <CCowTreeConfig TConfig>
void TCowTree<TConfig>::clear()
{
    YT_VERIFY(IsMaster());
    if (Height_ > 0) {
        Core_->Version++;
        EvictLeafIterators(Root_.Get(), Height_ - 1);
        Root_.Reset();
        Height_ = 0;
        Size_ = 0;
        MaxKeyHolder_.Clear();
    }
}

template <CCowTreeConfig TConfig>
template <std::input_iterator TInputIt>
void TCowTree<TConfig>::insert(TInputIt first, TInputIt last)
{
    for (; first != last; ++first) {
        if constexpr (HasValue) {
            auto&& element = *first;
            const auto& key = element.first;
            Insert(key, std::forward<decltype(element)>(element).second);
        } else {
            Insert(*first);
        }
    }
}

template <CCowTreeConfig TConfig>
void TCowTree<TConfig>::insert(std::initializer_list<value_type> elements)
{
    insert(elements.begin(), elements.end());
}

template <CCowTreeConfig TConfig>
typename TConfig::TLess TCowTreeBase<TConfig>::key_comp() const
{
    return Less_;
}

template <CCowTreeConfig TConfig>
auto TCowTreeBase<TConfig>::value_comp() const
{
    if constexpr (HasValue) {
        return [less = Less_] (const value_type& lhs, const value_type& rhs) {
            return less(lhs.first, rhs.first);
        };
    } else {
        return Less_;
    }
}

template <CCowTreeConfig TConfig>
bool TCowTreeBase<TConfig>::operator==(const TCowTreeBase& other) const
{
    // Shared roots mean shared content: a snapshot equals its unmutated master
    // (and another snapshot of the same version) in O(1).
    if (Root_.Get() == other.Root_.Get()) {
        return true;
    }
    if (Size_ != other.Size_) {
        return false;
    }
    auto left = begin();
    auto right = other.begin();
    for (const auto finish = end(); left != finish; ++left, ++right) {
        if constexpr (HasValue) {
            if (!(left->first == right->first) || !(left->second == right->second)) {
                return false;
            }
        } else {
            if (!(*left == *right)) {
                return false;
            }
        }
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

template <CCowTreeConfig TConfig>
bool TCowTreeBase<TConfig>::KeysEqual(const TKey& lhs, const TKey& rhs) const
{
    return !Less_(lhs, rhs) && !Less_(rhs, lhs);
}

template <CCowTreeConfig TConfig>
auto TCowTree<TConfig>::SelfCheckNode(TNode* node, size_t moreLevels, size_t& calculatedSize, size_t& leafCount, size_t& separatorCount) -> const TKeyHolder*
{
    YT_ASSERT(node->Version > 0);
    YT_ASSERT(node->Version <= Core_->Version);
    if (moreLevels == 0) {
        TLeaf* leaf = static_cast<TLeaf*>(node);
        YT_ASSERT(leaf->IsLeaf);
        YT_ASSERT(leaf->Size <= MaxNodeSize);
        YT_ASSERT(leaf->Size > 0);
        for (int i = 1; i < leaf->Size; i++) {
            YT_ASSERT(Less_(leaf->Keys[i - 1].Get(), leaf->Keys[i].Get()));
        }
        calculatedSize += leaf->Size;
        leafCount++;
        return &leaf->Keys[leaf->Size - 1];
    }
    TInner* inner = static_cast<TInner*>(node);
    YT_ASSERT(!inner->IsLeaf);
    YT_ASSERT(inner->Size >= 2);
    YT_ASSERT(inner->Size <= MaxNodeSize);
    separatorCount += inner->Size - 1;
    for (int i = 2; i < inner->Size; i++) {
        YT_ASSERT(Less_(inner->Keys[i - 1].Get(), inner->Keys[i].Get()));
    }
    const TKeyHolder* childMax = nullptr;
    for (int i = 0; i < inner->Size; ++i) {
        childMax = SelfCheckNode(inner->Children[i], moreLevels - 1, calculatedSize, leafCount, separatorCount);
        if (i + 1 < inner->Size) {
            YT_ASSERT(KeysEqual(inner->Keys[i + 1].Get(), childMax->Get()));
        }
    }
    return childMax;
}

template <CCowTreeConfig TConfig>
template <std::input_iterator TInputIt>
void TCowTree<TConfig>::BuildFromSorted(TInputIt first, TInputIt last)
{
    YT_VERIFY(IsMaster());
    YT_VERIFY(Height_ == 0);
    if (first == last) {
        return;
    }
    ui64 version = ++Core_->Version;

    // Pack the stream into fully filled leaves, verifying the order on the fly. The
    // staged forest is owned by |level| and |parents| — each entry owns its subtree,
    // adopted entries are nulled out — until the finished tree takes it over; the
    // guard releases whatever is left there if any step below throws.
    std::vector<TNode*> level;
    std::vector<TNode*> parents;
    Y_DEFER
    {
        for (auto* node : level) {
            if (node) {
                TNodeOps::Unref(node);
            }
        }
        for (auto* node : parents) {
            if (node) {
                TNodeOps::Unref(node);
            }
        }
    };
    size_t total = 0;
    TLeaf* leaf = nullptr;
    const TKey* previousKey = nullptr;
    for (; first != last; ++first) {
        if (!leaf || leaf->Size == MaxNodeSize) {
            // The slot is reserved first, so the fresh leaf is owned right away.
            level.push_back(nullptr);
            leaf = NCowTreeInternals::CreateCowTreeNode<TConfig, TLeaf>();
            level.back() = leaf;
            leaf->Version = version;
        }
        int pos = leaf->Size;
        auto&& element = *first;
        if constexpr (HasValue) {
            leaf->Keys[pos].Set(element.first);
            leaf->Values[pos].Set(element.second);
        } else {
            leaf->Keys[pos].Set(element);
        }
        leaf->Size = pos + 1;
        YT_VERIFY(previousKey == nullptr || Less_(*previousKey, leaf->Keys[pos].Get()));
        previousKey = &leaf->Keys[pos].Get();
        ++total;
    }

    // The input size is unknown up front, so the leaf level balances its tail
    // after the fact: the last leaf borrows from its (full) neighbor up to the
    // half-node bar.
    if (level.size() > 1) {
        auto* neighbor = static_cast<TLeaf*>(level[level.size() - 2]);
        auto* tail = static_cast<TLeaf*>(level.back());
        int deficit = MaxNodeSize / 2 - tail->Size;
        if (deficit > 0) {
            NCowTreeInternals::NodeDataMove<true>(tail, 0, tail->Size, tail, tail->Size + deficit);
            NCowTreeInternals::NodeDataMove<false>(neighbor, neighbor->Size - deficit, neighbor->Size, tail, 0);
            neighbor->Size -= deficit;
            tail->Size += deficit;
        }
    }

    std::vector<const TKeyHolder*> maxima;
    maxima.reserve(level.size());
    for (auto* node : level) {
        auto* builtLeaf = static_cast<TLeaf*>(node);
        maxima.push_back(&builtLeaf->Keys[builtLeaf->Size - 1]);
    }

    // Build the inner levels bottom-up; the level sizes are known now, so the
    // tail is planned instead of patched: a trailing remainder under half a
    // node is split evenly with its neighbor.
    int height = 1;
    while (level.size() > 1) {
        std::vector<const TKeyHolder*> parentMaxima;
        size_t childIndex = 0;
        while (childIndex < level.size()) {
            size_t remaining = level.size() - childIndex;
            int take = remaining <= static_cast<size_t>(MaxNodeSize)
                ? static_cast<int>(remaining)
                : remaining < static_cast<size_t>(MaxNodeSize + MaxNodeSize / 2)
                ? static_cast<int>((remaining + 1) / 2)
                : MaxNodeSize;
            parents.push_back(nullptr);
            auto* inner = NCowTreeInternals::CreateCowTreeNode<TConfig, TInner>();
            parents.back() = inner;
            inner->Version = version;
            for (int i = 0; i < take; ++i, ++childIndex) {
                if (i > 0) {
                    inner->Keys[i].CopyFrom(*maxima[childIndex - 1]);
                }
                // The child slot adopts the node's birth reference unit; the size
                // grows in step, so the node owns exactly what it already holds.
                inner->Children[i] = level[childIndex];
                level[childIndex] = nullptr;
                inner->Size = i + 1;
            }
            parentMaxima.push_back(maxima[childIndex - 1]);
        }
        level = std::move(parents);
        parents.clear();
        maxima = std::move(parentMaxima);
        ++height;
    }

    YT_VERIFY(height <= MaxHeight);
    MaxKeyHolder_.CopyFrom(*maxima.front());
    Root_ = TNodeRef(level.front());
    level.clear();
    Height_ = height;
    Size_ = total;
}

template <CCowTreeConfig TConfig>
void TCowTree<TConfig>::SelfCheck()
{
    YT_VERIFY(IsMaster());
    if (Height_ > 0) {
        size_t calculatedSize = 0;
        size_t leafCount = 0;
        size_t separatorCount = 0;
        [[maybe_unused]] const auto* rootMax = SelfCheckNode(Root_.Get(), Height_ - 1, calculatedSize, leafCount, separatorCount);
        YT_ASSERT(KeysEqual(MaxKeyHolder_.Get(), rootMax->Get()));
        // Each leaf's max is stored in exactly one extra place: a separator or MaxKeyHolder_.
        YT_ASSERT(separatorCount == leafCount - 1);
        YT_ASSERT(Size_ == calculatedSize);
    } else {
        YT_ASSERT(Size_ == 0);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
