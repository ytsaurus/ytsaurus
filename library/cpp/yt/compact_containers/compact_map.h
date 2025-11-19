#pragma once

#include "compact_vector.h"

#include <map>
#include <initializer_list>
#include <tuple>
#include <utility>
#include <algorithm>
#include <type_traits>

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

//! A flat map that keeps up to N elements inline in a sorted TCompactVector,
//! then transparently upgrades to std::map for larger sizes.
//! Any modifying operation may invalidate all iterators.
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
    // We store std::pair<TKey, TValue> in TCompactVector because of its copy-assignability requirement.
    // The underlying storage type of std::map is, obviosly, std::pair<const TKey, TValue>.
    // To provide a single iterator interface, we use reinterpret_cast to convert between pointers
    // to values of these types, which should have the same layout on all sane compilers.
    // However, this is still technically undefined behavior and a strict aliasing violation.
    // Annotating it with may_alias should stop optimizations based on type-based alias analysis.
    using value_type = std::pair<const TKey, TValue> alias_hack;
    using key_compare = TCompare;
    using allocator_type = TAllocator;
    using size_type = std::size_t;

    class iterator;
    class const_iterator;

    TCompactMap() = default;
    explicit TCompactMap(const allocator_type& alloc);

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

    // Inline vector stores pairs without const-qualified key, because its values have to be copy-assignable.
    using TVector = TCompactVector<std::pair<TKey, TValue>, N>;
    using TVectorValue = typename TVector::value_type;
    using TMap = std::map<key_type, mapped_type, key_compare, allocator_type>;
    using TVectorIterator = typename TVector::iterator;
    using TVectorConstIterator = typename TVector::const_iterator;
    using TMapIterator = typename TMap::iterator;
    using TMapConstIterator = typename TMap::const_iterator;

    TVector Vector_;
    TMap Map_;
    key_compare Compare_{};
    allocator_type Alloc_{};

    // Returns if map part is empty (i.e. we are still using inline vector).
    bool IsSmall() const;
    // Moves data from inline vector to map.
    void UpgradeToMap();

    // Performs binary search in inline vector. Returns iterator and found flag.
    std::pair<TVectorConstIterator, bool> VectorLowerBound(const key_type& key) const;
    std::pair<TVectorIterator, bool> VectorLowerBound(const key_type& key);

    template <class... TArgs>
    TVectorIterator EmplaceSmall(TVectorConstIterator pos, TArgs&&... args);

    template <class TVectorIteratorType>
    static std::pair<TVectorIteratorType, bool> VectorLowerBoundImpl(
        TVectorIteratorType begin,
        TVectorIteratorType end,
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
