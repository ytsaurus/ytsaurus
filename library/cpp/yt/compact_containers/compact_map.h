#pragma once

#include <array>
#include <map>
#include <initializer_list>
#include <tuple>
#include <utility>
#include <algorithm>
#include <memory>
#include <type_traits>

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

//! A flat map that keeps up to N elements inline in a sorted buffer,
//! similar to TCompactFlatMap, but, unlike TCompactFlatMap, transparently
//! falls back to std::map storage when the size exceeds N.
//!
//! Be very careful, any modifying operation may invalidate all iterators.
template <
    class TKey,
    class TValue,
    size_t N,
    class TCompare = std::less<TKey>,
    class TAllocator = std::allocator<std::pair<const TKey, TValue>>>
class TCompactMap
{
public:
    using key_type = TKey;
    using mapped_type = TValue;
    using value_type = std::pair<const TKey, TValue>;
    using key_compare = TCompare;
    using allocator_type = TAllocator;
    using size_type = std::size_t;

    class iterator;
    class const_iterator;

    TCompactMap() = default;
    explicit TCompactMap(const allocator_type& alloc);
    TCompactMap(const TCompactMap& other);
    TCompactMap(TCompactMap&& other) noexcept(
        std::is_nothrow_move_constructible_v<value_type> &&
        std::is_nothrow_move_constructible_v<key_compare> &&
        std::allocator_traits<allocator_type>::is_always_equal::value);
    TCompactMap& operator=(const TCompactMap& other);
    TCompactMap& operator=(TCompactMap&& other) noexcept(
        std::is_nothrow_move_constructible_v<value_type> &&
        std::is_nothrow_move_constructible_v<key_compare> &&
        std::allocator_traits<allocator_type>::is_always_equal::value);
    ~TCompactMap();

    template <class TIt>
    TCompactMap(TIt first, TIt last);

    TCompactMap(std::initializer_list<value_type> init);

    bool empty() const;
    size_type size() const;

    void clear();

    iterator begin();
    const_iterator begin() const;
    const_iterator cbegin() const;

    iterator end();
    const_iterator end() const;
    const_iterator cend() const;

    size_type count(const key_type& key) const;

    iterator find(const key_type& key);
    const_iterator find(const key_type& key) const;

    bool contains(const key_type& key) const;

    std::pair<iterator, bool> insert(const value_type& v);

    template <class... TArgs>
    std::pair<iterator, bool> emplace(TArgs&&... args);

    template <class... TArgs>
    std::pair<iterator, bool> try_emplace(const key_type& key, TArgs&&... args);

    template <class... TArgs>
    std::pair<iterator, bool> try_emplace(key_type&& key, TArgs&&... args);

    template <class TIt>
    void insert(TIt first, TIt last);

    template <class TMapped>
    std::pair<iterator, bool> insert_or_assign(const key_type& key, TMapped&& obj);

    size_type erase(const key_type& key);
    iterator erase(iterator pos);
    iterator erase(const_iterator pos);
    iterator erase(const_iterator first, const_iterator last);

    mapped_type& operator[](const key_type& key);
    mapped_type& operator[](key_type&& key);

    mapped_type& at(const key_type& key);
    const mapped_type& at(const key_type& key) const;

private:
    template <bool IsConst>
    class TIteratorBase;

    using TArrayValue = value_type;
    using TStorage = std::aligned_storage_t<sizeof(TArrayValue), alignof(TArrayValue)>;
    using TArrayIterator = TArrayValue*;
    using TArrayConstIterator = const TArrayValue*;
    using TInlineStorage = std::array<TStorage, N>;
    using TMap = std::map<key_type, mapped_type, key_compare, allocator_type>;
    using TMapIterator = typename TMap::iterator;
    using TMapConstIterator = typename TMap::const_iterator;

    TInlineStorage ArrayStorage_;
    size_type ArraySize_ = 0;
    TMap Map_;
    key_compare Compare_{};
    allocator_type Alloc_{};

    TArrayIterator ArrayData();
    TArrayConstIterator ArrayData() const;
    TArrayIterator ArrayBegin();
    TArrayConstIterator ArrayBegin() const;
    TArrayIterator ArrayEnd();
    TArrayConstIterator ArrayEnd() const;
    void ClearArray();
    void CopyArrayFrom(const TCompactMap& other);
    void MoveArrayFrom(TCompactMap&& other);

    // Returns if map part is empty (i.e. we are still using inline array).
    bool IsSmall() const;
    // Moves data from inline array to map.
    void UpgradeToMap();

    // Performs binary search in inline array. Returns iterator and found flag.
    std::pair<TArrayConstIterator, bool> ArrayLowerBound(const key_type& key) const;
    std::pair<TArrayIterator, bool> ArrayLowerBound(const key_type& key);

    template <class... TArgs>
    TArrayIterator EmplaceSmall(TArrayConstIterator pos, TArgs&&... args);
    TArrayIterator EraseSmall(TArrayConstIterator pos);
    TArrayIterator EraseSmall(TArrayConstIterator first, TArrayConstIterator last);

    template <class TArrayIteratorType>
    static std::pair<TArrayIteratorType, bool> ArrayLowerBoundImpl(
        TArrayIteratorType begin,
        TArrayIteratorType end,
        const key_type& key,
        const key_compare& compare);

    template <class TKeyParam>
    mapped_type& Subscript(TKeyParam&& key);

    template <class TSelf>
    static auto BeginImpl(TSelf* self)
        -> std::conditional_t<std::is_const_v<TSelf>, const_iterator, iterator>;

    template <class TSelf>
    static auto EndImpl(TSelf* self)
        -> std::conditional_t<std::is_const_v<TSelf>, const_iterator, iterator>;

    template <class TSelf>
    static auto FindImpl(TSelf* self, const key_type& key)
        -> std::conditional_t<std::is_const_v<TSelf>, const_iterator, iterator>;

    template <class TSelf>
    static auto AtImpl(TSelf* self, const key_type& key)
        -> std::conditional_t<std::is_const_v<TSelf>, const mapped_type&, mapped_type&>;

    template <class TKeyParam, class... TArgs>
    std::pair<iterator, bool> TryEmplaceImpl(TKeyParam&& key, TArgs&&... args);

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define COMPACT_MAP_INL_H_
#include "compact_map-inl.h"
#undef COMPACT_MAP_INL_H_
