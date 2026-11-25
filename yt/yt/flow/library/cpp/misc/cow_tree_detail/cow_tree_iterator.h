#pragma once

#include "cow_tree_config.h"
#include "cow_tree_node.h"
#include "cow_tree_value.h"

#include <util/system/compiler.h>
#include <util/system/types.h>

#include <iterator>
#include <type_traits>
#include <utility>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <CCowTreeConfig TConfig>
class TCowTreeBase;

template <CCowTreeConfig TConfig>
class TCowTree;

template <CCowTreeConfig TConfig>
class TCowTreeSnapshot;

template <CCowTreeConfig TConfig, bool IsConst>
class TCowTreeIterator;

//! The state and navigation shared by both iterator flavours of a tree. A
//! snapshot's iterator walks frozen nodes and never validates anything. A
//! master's iterator (const or not) registers in the tracked list of its current
//! leaf, and every mutation repairs the registered iterators of the leaves it
//! touches in place — so an iterator is simply always valid: it keeps denoting
//! the same element through any mutations (the successor, once its own element
//! is erased), and the hot paths carry no validation at all. Leaf-to-leaf
//! transitions hop through the remembered parent; a full scan costs
//! O(n + (n / MaxNodeSize) * log n).
template <CCowTreeConfig TConfig>
class TCowTreeIteratorBase
{
public:
    using TTree = TCowTreeBase<TConfig>;
    using TKey = typename TConfig::TKey;
    using TValue = typename TConfig::TValue;
    using TLeaf = NCowTreeInternals::TLeaf<TConfig>;

    static constexpr bool HasValue = !std::is_void_v<TValue>;

    TCowTreeIteratorBase() = default;

    TCowTreeIteratorBase(const TCowTreeIteratorBase& other);
    TCowTreeIteratorBase(TCowTreeIteratorBase&& other) noexcept;
    TCowTreeIteratorBase& operator=(const TCowTreeIteratorBase& other);
    TCowTreeIteratorBase& operator=(TCowTreeIteratorBase&& other) noexcept;

    ~TCowTreeIteratorBase();

    //! Comparison is positional; since iterators are always kept valid, it is the
    //! exact element (or end()) identity. The const and mutable flavours compare
    //! with each other freely.
    bool operator==(const TCowTreeIteratorBase& other) const;

protected:
    friend class TCowTreeBase<TConfig>;
    friend class TCowTree<TConfig>;
    friend class TCowTreeSnapshot<TConfig>;
    friend class TCowTreeIterator<TConfig, false>;
    friend class TCowTreeIterator<TConfig, true>;

    mutable TTree* Tree_ = nullptr;
    mutable TLeaf* Leaf_ = nullptr;
    //! i64 deliberately: with a 32-bit Pos_ the compiler fuses the sign-extending
    //! key-address load into the ++ chain of a scan loop, which defeats the CPU's
    //! store-forwarding elimination for the spilled iterator and serializes the
    //! whole scan on a ~10-cycle dependency.
    mutable i64 Pos_ = 0;
    //! The leaf's parent and the leaf's slot in it. The parent identity is repaired
    //! eagerly by every mutation that changes it (never null under a non-root leaf);
    //! the slot may drift as siblings come and go and is refound on demand at hop
    //! time. Null under a root leaf.
    mutable NCowTreeInternals::TInner<TConfig>* Parent_ = nullptr;
    mutable int ParentPos_ = 0;
    //! Set for a master's iterator: it sits in the tracked list of its current
    //! leaf and is repaired in place by every mutation that touches that leaf.
    mutable bool Tracking_ = false;
    mutable TCowTreeIteratorBase* PrevTracked_ = nullptr;
    mutable TCowTreeIteratorBase* NextTracked_ = nullptr;

    //! end() of |tree|.
    explicit TCowTreeIteratorBase(TTree* tree);

    TCowTreeIteratorBase(TTree* tree, TLeaf* leaf, int pos, NCowTreeInternals::TInner<TConfig>* parent, int parentPos);

    //! The head of the current leaf's tracked list (type-erased in the node).
    TCowTreeIteratorBase* LeafListHead() const;
    void SetLeafListHead(TCowTreeIteratorBase* head) const;

    void LinkToLeaf();
    void UnlinkFromLeaf();

    //! Repoints ParentPos_ at the leaf's current slot; the parent identity is
    //! guaranteed, only the slot may have drifted.
    void RepairParentSlot();

    //! Steps to an adjacent leaf through the parent.
    void StepToSibling(int parentPos);

    void Increment();
    void Decrement();

    Y_NO_INLINE void IncrementSlow();
    Y_NO_INLINE void DecrementSlow();

    void ResetState();
    void CopyState(const TCowTreeIteratorBase& other);
    void MoveState(TCowTreeIteratorBase& other);
};

////////////////////////////////////////////////////////////////////////////////

//! The std-shaped bidirectional tree iterator over the always-valid base above.
//! IsConst fixes the mapped-value access, exactly the std iterator/const_iterator
//! split: the const flavour (the only kind snapshots hand out) reads values by
//! plain const reference; the master's mutable flavour dereferences to a writable
//! TValue& — the std discipline of "a non-const iterator may write", so the access
//! counts as a modification and copies a leaf shared with a snapshot (once — the
//! copy is exclusive from then on). Sets read keys by const reference either way.
template <CCowTreeConfig TConfig, bool IsConst>
class TCowTreeIterator
    : public TCowTreeIteratorBase<TConfig>
{
public:
    using TBase = TCowTreeIteratorBase<TConfig>;
    using TKey = typename TBase::TKey;
    using TValue = typename TBase::TValue;

    static constexpr bool HasValue = TBase::HasValue;

    //! The pair-like element view of a map iterator. A prvalue proxy, not a
    //! reference into storage (the element physically lives in two parallel
    //! arrays); it converts to value_type so the copying algorithms work.
    struct TReference
    {
        const TKey& first; // NOLINT(readability-identifier-naming): std::pair interface.
        std::conditional_t<IsConst, const TValue, TValue>& second; // NOLINT(readability-identifier-naming): std::pair interface.

        //! Templated like std::pair's own converting machinery, so both
        //! value_type and plain std::pair<TKey, TValue> are one conversion away.
        template <class TFirst, class TSecond>
        operator std::pair<TFirst, TSecond>() const;
    };

    struct TArrowProxy
    {
        TReference Ref;

        TReference* operator->();
    };

    using difference_type = std::ptrdiff_t;
    using value_type = std::conditional_t<HasValue, std::pair<const TKey, TValue>, TKey>;
    using reference = std::conditional_t<HasValue, TReference, const TKey&>;
    using pointer = std::conditional_t<HasValue, TArrowProxy, const TKey*>;
    using iterator_category = std::bidirectional_iterator_tag;

    TCowTreeIterator() = default;

    //! iterator converts to const_iterator, never back. Templated so that it
    //! never doubles as the copy constructor (which stays implicitly defaulted).
    template <bool OtherConst>
    TCowTreeIterator(const TCowTreeIterator<TConfig, OtherConst>& other)
        requires(IsConst && !OtherConst);

    reference operator*() const;
    pointer operator->() const;

    TCowTreeIterator& operator++();
    TCowTreeIterator operator++(int);
    TCowTreeIterator& operator--();
    TCowTreeIterator operator--(int);

private:
    friend class TCowTreeBase<TConfig>;
    friend class TCowTree<TConfig>;
    friend class TCowTreeSnapshot<TConfig>;

    //! Adopts the state produced by the trees' seek machinery.
    explicit TCowTreeIterator(TBase&& base);

    //! end() of |tree| in one construction — end() sits on scan hot paths.
    explicit TCowTreeIterator(typename TBase::TTree* tree);

    // The dependent TResult keeps a void& declaration out of set instantiations.
    template <class TResult = TValue>
    TResult& MutableValue() const
        requires(HasValue && !IsConst);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define COW_TREE_ITERATOR_INL_H
#include "cow_tree_iterator-inl.h"
#undef COW_TREE_ITERATOR_INL_H
