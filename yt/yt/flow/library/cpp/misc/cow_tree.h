#pragma once

#include <concepts>
#include <iostream>

#include "public.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

//! Stores the current version of the Tree (when snapshot is performed, new version context is performed).
//! Version is incremented every time a node is CoW-ed rather than being modified inplace.
//! If the versions match at any two moments, the tree structure had not changed between them.
//! It is thus safe to use cached position to perform increments and decrements.
//! Important: CoWs on the level of individual values within a leaf node are not handled by VersionContext.
class TCowTreeVersionContext
{
public:
    void Increment();
    size_t GetVersion() const;

private:
    size_t Version_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

// This section contains forward declarations for some internal implementation details.
// All actual implementations are in -inl.h file.

template <class T>
class TMaybeInplaceIntrusivePtr;

template <class TValue>
class TCowTreeValueWrapper;

template <class TKey, class TValue, size_t BlockSize, class TLess>
struct TCowTreeCachedPosition;

template <class TKey, class TValue, size_t BlockSize, class TLess>
class TCowTreeNode;

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

//! Useful functions for working with keys & values.
template <class TKey, class TValue>
struct TCowTreeTraits
{
    using value_type = std::pair<const TKey, TValue>;
    //! Designated ref_value_type which will be returned by iterators dereference is needed since we don't want to actually store std::pair<const TKey, TValue> objects anywhere.
    using ref_value_type = std::pair<const TKey, TValue&>;
    using const_ref_value_type = std::pair<const TKey, const TValue&>;
    using ptr_value_type = std::optional<ref_value_type>;
    using const_ptr_value_type = std::optional<const_ref_value_type>;
    //! Wrapper is a reasonable (without any references) lightweight key/value pair inside the tree.
    using TWrapper = std::pair<TKey, NDetail::TCowTreeValueWrapper<TValue>*>;

    static const TKey& KeyFromKV(const value_type& kv);

    static TValue& ValueFromKV(const value_type& kv);
    static TValue&& ValueFromKV(value_type&& kv);

    static ref_value_type KVFromWrapper(const TWrapper& wrapper);
    static const_ref_value_type ConstKVFromWrapper(const TWrapper& wrapper);
    static ptr_value_type PtrFromWrapper(const TWrapper& wrapper);
    static const_ptr_value_type ConstPtrFromWrapper(const TWrapper& wrapper);
    static const TKey& KeyFromWrapper(const TWrapper& wrapper);
};

template <class TKey>
struct TCowTreeTraits<TKey, void>
{
    using value_type = const TKey;
    using ref_value_type = const TKey;
    using const_ref_value_type = const TKey;
    using ptr_value_type = std::optional<const TKey>;
    using const_ptr_value_type = std::optional<const TKey>;
    using TWrapper = TKey;

    static const TKey& KeyFromKV(const value_type& kv);

    static ref_value_type KVFromWrapper(const TWrapper& wrapper);
    static const_ref_value_type ConstKVFromWrapper(const TWrapper& wrapper);
    static ptr_value_type PtrFromWrapper(const TWrapper& wrapper);
    static const_ptr_value_type ConstPtrFromWrapper(const TWrapper& wrapper);
    static const TKey& KeyFromWrapper(const TWrapper& wrapper);
};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, size_t BlockSize, class TLess = std::less<TKey>>
class TCowTree;

static constexpr size_t DefaultBlockSize = 16;

template <class TKey, class TLess>
using TCowSet = TCowTree<TKey, void, DefaultBlockSize, TLess>;

template <class TKey, class TValue, class TLess>
using TCowMap = TCowTree<TKey, TValue, DefaultBlockSize, TLess>;

//! This is the implementation of a copy-on-write (persistent) B-Tree.
//!
//! Template argument BlockSize is the maximum children/keys per-node.
//! Increasing BlockSize might speed up operations by increasing locality, but would require more space and time for actually making nodes copies.
//!
//! Semantics of std::set/std::map are largely preserved.
//! For example, it is safe to perform non-modifying actions on the same instance concurrently, but unsafe to run any modifying operation concurrently with anything else.
//! Copying the tree uses CoW technique and thus is performed in O(1) time.
//! After that, despite the trees sharing large parts of their internal structure, in terms of API they may be considered fully independent. For example it is now safe to run concurrently modifying operations in these two trees.
//! However, there are some discrepancies due to the restrictions imposed by CoW approach, described below.
//!
//! Note that accessing non-const iterator (by using non-const find, at etc) leads to copy with the corresponding cost.
//! So it could be cheaper to use const methods (like std::as_const(tree).find(..) ).
template <class TKey, class TValue, size_t BlockSize, class TLess>
class TCowTree
{
    static_assert(BlockSize > 0 && BlockSize % 2 == 0);
    using TWrapper = typename TCowTreeTraits<TKey, TValue>::TWrapper;

public:
    using value_type = typename TCowTreeTraits<TKey, TValue>::value_type;
    using key_type = TKey;

private:
    class TNodeHandle
    {
        friend class TCowTree;

    public:
        const value_type& value() const&;
        value_type& value() &;
        value_type&& value() &&;
        bool empty() const;

    private:
        std::optional<value_type> Value_;
        TNodeHandle(value_type&& kv);
    };

public:
    using node_type = TNodeHandle;

    //! Due to the complications of CoW approach semantics and guaranteed of the iterators are slightly different compared to their std counterparts.
    //!
    //! When TValue is void (std::set analog):
    //! iterator and const_iterator are the same type, invalidations happens exactly as in std::set.
    //! Dereference will always take O(1) time, increments and decrements will take O(1)/O(nlogn).
    //! Example: if no modifications are performed, increment/decrement will work in O(1) time except when shifting between different leaf nodes.
    //!
    //! When TValue is not void (std::map analog):
    //! const_iterator does't allow modifications of the underlying value and is also invalidated upon any modification operation in CowTree.
    //! iterator does allow modification of the underlying value and the rules of invalidation match std::map with added invalidation upon snapshot.
    //! Operations involving iterator, however, are substantially more expensive since they ensure the unique ownership of the value, potentially performing multiple CoW operations.
    template <bool Modifiable>
    class TIterator
    {
        friend class TCowTree;

    public:
        using difference_type = std::ptrdiff_t;
        using value_type = typename TCowTreeTraits<TKey, TValue>::value_type;
        using reference = std::conditional_t<Modifiable, typename TCowTreeTraits<TKey, TValue>::ref_value_type, typename TCowTreeTraits<TKey, TValue>::const_ref_value_type>;
        using pointer = std::conditional_t<Modifiable, typename TCowTreeTraits<TKey, TValue>::ptr_value_type, typename TCowTreeTraits<TKey, TValue>::const_ptr_value_type>;
        using iterator_category = std::bidirectional_iterator_tag;

        TIterator();

        reference operator*() const;
        pointer operator->() const;

        bool operator==(const TIterator& other) const;
        bool operator!=(const TIterator& other) const;

        TIterator& operator++();
        TIterator operator++(int);
        TIterator& operator--();
        TIterator operator--(int);

        operator TIterator<false>() const&;
        operator TIterator<false>() &&;

    private:
        std::optional<NDetail::TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>> CachedPosition_;
        const TCowTree* Tree_;
        TLess Less_;

        TIterator(std::optional<NDetail::TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>> node, const TCowTree* tree);
    };

    using iterator = std::conditional_t<std::is_void_v<TValue>, TIterator<false>, TIterator<true>>;
    using const_iterator = TIterator<false>;

public:
    const_iterator cbegin() const;
    const_iterator begin() const;
    iterator begin();

    const_iterator cend() const;
    const_iterator end() const;
    iterator end();

    const_iterator find(const TKey& key) const;
    template <class TAnyKey>
    const_iterator find(const TAnyKey& key) const;
    iterator find(const TKey& key);
    template <class TAnyKey>
    iterator find(const TAnyKey& key);

    bool contains(const TKey& key) const;
    template <class TAnyKey>
    bool contains(const TAnyKey& key) const;

    template <std::convertible_to<typename TCowTreeTraits<TKey, TValue>::value_type> TAnyKeyValue>
    std::pair<iterator, bool> insert(TAnyKeyValue&& kv);
    template <std::convertible_to<typename TCowTreeTraits<TKey, TValue>::value_type> TAnyKeyValue>
    iterator insert(const_iterator, TAnyKeyValue&& kv);

    std::pair<iterator, bool> insert(const value_type& kv);
    std::pair<iterator, bool> insert(value_type&& kv);

    iterator insert(const_iterator it, const value_type& kv);
    iterator insert(const_iterator it, value_type&& kv);

    void insert(node_type&& node);

    size_t erase(const TKey& key);
    void erase(const_iterator it);

    bool empty() const;

    size_t size() const;

    node_type extract(const TKey& key);
    node_type extract(const_iterator it);

    size_t count(const TKey& key) const;
    template <class TAnyKey>
    size_t count(const TAnyKey& key) const;

    void clear();

    const_iterator lower_bound(const TKey& key) const;
    template <class TAnyKey>
    const_iterator lower_bound(const TAnyKey& key) const;
    const_iterator upper_bound(const TKey& key) const;
    template <class TAnyKey>
    const_iterator upper_bound(const TAnyKey& key) const;
    iterator lower_bound(const TKey& key);
    template <class TAnyKey>
    iterator lower_bound(const TAnyKey& key);
    iterator upper_bound(const TKey& key);
    template <class TAnyKey>
    iterator upper_bound(const TAnyKey& key);

    typename std::add_lvalue_reference_t<const TValue> at(const TKey& key) const
        requires(!std::is_void_v<TValue>);
    typename std::add_lvalue_reference_t<TValue> at(const TKey& key)
        requires(!std::is_void_v<TValue>);

    //! Verify all internal invariants are satisfied. Used primarily for testing.
    void Verify() const;
    iterator FindFirstSatisfying(const std::function<bool(const TKey&)>& predicate);

private:
    NDetail::TMaybeInplaceIntrusivePtr<NDetail::TCowTreeNode<TKey, TValue, BlockSize, TLess>> Root_ = nullptr;
    NDetail::TCowTreeVersionContext VersionContext_;
    TLess Less_;

    template <std::convertible_to<typename TCowTreeTraits<TKey, TValue>::value_type> TAnyKeyValue, bool OverrideValue = false>
    std::pair<NDetail::TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>, bool> Insert(TAnyKeyValue&& keyValue);

    //! Returns the count (0/1) of the erased elements.
    size_t Erase(const TKey& key);

    template <bool ForModify>
    std::optional<NDetail::TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>> FirstSatisfying(const std::function<bool(const TKey&)>& predicate) const;
    template <bool ForModify>
    std::optional<NDetail::TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>> LastSatisfying(const std::function<bool(const TKey&)>& predicate) const;
    size_t Size() const;
};

} // namespace NYT::NFlow

#define COW_TREE_H_
#include "cow_tree-inl.h"
#undef COW_TREE_H_
