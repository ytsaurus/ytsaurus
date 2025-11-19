#ifndef COMPACT_MAP_INL_H_
#error "Direct inclusion of this file is not allowed, include compact_map.h"
#include "compact_map.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
template <bool IsConst>
class TCompactMap<TKey, TValue, N, TCompare, TAllocator>::TIteratorBase
{
private:
    friend class TCompactMap<TKey, TValue, N, TCompare, TAllocator>;
    friend class TIteratorBase<!IsConst>;
    friend class iterator;
    friend class const_iterator;

    using TVecIter = std::conditional_t<IsConst, TVectorConstIterator, TVectorIterator>;
    using TMapIter = std::conditional_t<IsConst, TMapConstIterator, TMapIterator>;
    using TValueType = typename TCompactMap::value_type;
    using TVectorValue = typename TCompactMap::TVectorValue;
    using TSmallValue = std::conditional_t<IsConst, const TVectorValue, TVectorValue>;
    using TSmallPointer = std::conditional_t<IsConst, const TVectorValue*, TVectorValue*>;
    using TReference = std::conditional_t<IsConst, const TValueType&, TValueType&>;
    using TPointer = std::conditional_t<IsConst, const TValueType*, TValueType*>;

    static_assert(
        sizeof(TVectorValue) == sizeof(TValueType),
        "TCompactMap relies on identical layout of internal and public pair types");
    static_assert(
        alignof(TVectorValue) == alignof(TValueType),
        "TCompactMap relies on identical layout of internal and public pair types");

    static TReference VectorRef(TSmallValue& value)
    {
        // Technically, this violates strict aliasing rules, and is, therefore, undefined behavior.
        // In practice, see the comment regarding value_type in the class header. It is defined
        // with `may_alias` attribute to suppress optimizations based on type-based alias analysis.
        return reinterpret_cast<TReference>(value);
    }

    static TPointer VectorPtr(TSmallPointer ptr)
    {
        // Technically, this violates strict aliasing rules, and is, therefore, undefined behavior.
        // In practice, see the comment regarding value_type in the class header. It is defined
        // with `may_alias` attribute to suppress optimizations based on type-based alias analysis.
        return reinterpret_cast<TPointer>(ptr);
    }

    union
    {
        TVecIter VIter;
        TMapIter MIter;
    };
    bool Small_;

    explicit TIteratorBase(TVecIter it)
        : VIter(it)
        , Small_(true)
    { }

    explicit TIteratorBase(TMapIter it)
        : MIter(it)
        , Small_(false)
    { }

    // Conversion constructor for const_iterator from iterator.
    template <bool WasConst, typename = std::enable_if_t<IsConst && !WasConst>>
    TIteratorBase(const TIteratorBase<WasConst>& other)
        : Small_(other.Small_)
    {
        if (Small_) {
            VIter = other.VIter;
        } else {
            MIter = other.MIter;
        }
    }

public:
    using difference_type = std::ptrdiff_t;
    using value_type = TValueType;
    using reference = TReference;
    using pointer = TPointer;
    using iterator_category = std::bidirectional_iterator_tag;

    reference operator*() const
    {
        if (Small_) {
            return VectorRef(*VIter);
        }
        return *MIter;
    }

    pointer operator->() const
    {
        if (Small_) {
            return VectorPtr(&*VIter);
        }
        return &*MIter;
    }

    TIteratorBase& operator++()
    {
        if (Small_) {
            ++VIter;
        } else {
            ++MIter;
        }
        return *this;
    }

    TIteratorBase operator++(int)
    {
        TIteratorBase tmp = *this;
        ++*this;
        return tmp;
    }

    TIteratorBase& operator--()
    {
        if (Small_) {
            --VIter;
        } else {
            --MIter;
        }
        return *this;
    }

    TIteratorBase operator--(int)
    {
        TIteratorBase tmp = *this;
        --*this;
        return tmp;
    }

    bool operator==(const TIteratorBase& other) const
    {
        if (Small_ != other.Small_) {
            return false;
        }
        return Small_ ? VIter == other.VIter : MIter == other.MIter;
    }

    bool operator!=(const TIteratorBase& other) const
    {
        return !(*this == other);
    }
};

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
class TCompactMap<TKey, TValue, N, TCompare, TAllocator>::iterator
    : public TCompactMap<TKey, TValue, N, TCompare, TAllocator>::template TIteratorBase<false>
{
public:
    iterator() = default;

private:
    friend class TCompactMap<TKey, TValue, N, TCompare, TAllocator>;
    friend class const_iterator;
    using TBase = typename TCompactMap<TKey, TValue, N, TCompare, TAllocator>::template TIteratorBase<false>;

    explicit iterator(TVectorIterator it)
        : TBase(it)
    { }

    explicit iterator(TMapIterator it)
        : TBase(it)
    { }
};

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
class TCompactMap<TKey, TValue, N, TCompare, TAllocator>::const_iterator
    : public TCompactMap<TKey, TValue, N, TCompare, TAllocator>::template TIteratorBase<true>
{
public:
    const_iterator() = default;
    
    // Allow implicit conversion from iterator to const_iterator.
    const_iterator(const iterator& other)
        : TBase(other.Small_ ? TBase(other.VIter) : TBase(other.MIter))
    { }

private:
    friend class TCompactMap<TKey, TValue, N, TCompare, TAllocator>;
    using TBase = typename TCompactMap<TKey, TValue, N, TCompare, TAllocator>::template TIteratorBase<true>;

    explicit const_iterator(TVectorConstIterator it)
        : TBase(it)
    { }

    explicit const_iterator(TMapConstIterator it)
        : TBase(it)
    { }
};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::TCompactMap(const allocator_type& alloc)
    : Alloc_(alloc)
    , Map_(key_compare{}, alloc)
{ }

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
template <class TIt>
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::TCompactMap(TIt first, TIt last)
{
    insert(first, last);
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::TCompactMap(std::initializer_list<value_type> init)
{
    insert(init.begin(), init.end());
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
bool TCompactMap<TKey, TValue, N, TCompare, TAllocator>::empty() const
{
    return IsSmall() ? Vector_.empty() : Map_.empty();
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
typename TCompactMap<TKey, TValue, N, TCompare, TAllocator>::size_type
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::size() const
{
    return IsSmall() ? Vector_.size() : Map_.size();
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
void TCompactMap<TKey, TValue, N, TCompare, TAllocator>::clear()
{
    Vector_.clear();
    Map_.clear();
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::begin() -> iterator
{
    return BeginImpl(this);
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::begin() const -> const_iterator
{
    return BeginImpl(this);
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::cbegin() const -> const_iterator
{
    return begin();
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::end() -> iterator
{
    return EndImpl(this);
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::end() const -> const_iterator
{
    return EndImpl(this);
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::cend() const -> const_iterator
{
    return end();
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
bool TCompactMap<TKey, TValue, N, TCompare, TAllocator>::IsSmall() const
{
    return Map_.empty();
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
void TCompactMap<TKey, TValue, N, TCompare, TAllocator>::UpgradeToMap()
{
    if (!IsSmall()) {
        return;
    }

    Map_ = TMap(Compare_, Alloc_);
    for (auto& item : Vector_) {
        Map_.emplace(item.first, item.second);
    }

    Vector_.clear();
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
std::pair<typename TCompactMap<TKey, TValue, N, TCompare, TAllocator>::TVectorConstIterator, bool>
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::VectorLowerBound(const key_type& key) const
{
    return VectorLowerBoundImpl(Vector_.begin(), Vector_.end(), key, Compare_);
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
std::pair<typename TCompactMap<TKey, TValue, N, TCompare, TAllocator>::TVectorIterator, bool>
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::VectorLowerBound(const key_type& key)
{
    return VectorLowerBoundImpl(Vector_.begin(), Vector_.end(), key, Compare_);
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
template <class... TArgs>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::EmplaceSmall(TVectorConstIterator pos, TArgs&&... args) -> TVectorIterator
{
    return Vector_.emplace(pos, std::forward<TArgs>(args)...);
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
template <class TVectorIteratorType>
std::pair<TVectorIteratorType, bool>
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::VectorLowerBoundImpl(
    TVectorIteratorType begin,
    TVectorIteratorType end,
    const key_type& key,
    const key_compare& compare)
{
    auto comp = [&compare] (const auto& pair, const key_type& k) {
        return compare(pair.first, k);
    };

    auto it = std::lower_bound(begin, end, key, comp);
    bool found = (it != end && !compare(key, it->first));
    return {it, found};
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
template <class TKeyParam>
typename TCompactMap<TKey, TValue, N, TCompare, TAllocator>::mapped_type&
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::Subscript(TKeyParam&& key)
{
    if (IsSmall()) {
        auto [vectorIt, found] = VectorLowerBound(key);

        if (found) {
            return vectorIt->second;
        }

        if (Vector_.size() < N) {
            auto inserted = EmplaceSmall(vectorIt, std::forward<TKeyParam>(key), mapped_type());
            return inserted->second;
        }

        UpgradeToMap();
    }

    return Map_[std::forward<TKeyParam>(key)];
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
template <class TSelf>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::BeginImpl(TSelf* self)
    -> std::conditional_t<std::is_const_v<TSelf>, const_iterator, iterator>
{
    using TResult = std::conditional_t<std::is_const_v<TSelf>, const_iterator, iterator>;
    if (self->IsSmall()) {
        return TResult(self->Vector_.begin());
    }
    return TResult(self->Map_.begin());
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
template <class TSelf>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::EndImpl(TSelf* self)
    -> std::conditional_t<std::is_const_v<TSelf>, const_iterator, iterator>
{
    using TResult = std::conditional_t<std::is_const_v<TSelf>, const_iterator, iterator>;
    if (self->IsSmall()) {
        return TResult(self->Vector_.end());
    }
    return TResult(self->Map_.end());
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
template <class TSelf>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::FindImpl(TSelf* self, const key_type& key)
    -> std::conditional_t<std::is_const_v<TSelf>, const_iterator, iterator>
{
    using TResult = std::conditional_t<std::is_const_v<TSelf>, const_iterator, iterator>;
    if (self->IsSmall()) {
        auto [it, found] = self->VectorLowerBound(key);
        return found ? TResult(it) : TResult(self->Vector_.end());
    }
    return TResult(self->Map_.find(key));
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
template <class TSelf>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::AtImpl(TSelf* self, const key_type& key)
    -> std::conditional_t<std::is_const_v<TSelf>, const mapped_type&, mapped_type&>
{
    auto it = FindImpl(self, key);
    if (it == EndImpl(self)) {
        throw std::out_of_range("TCompactMap::at");
    }
    return it->second;
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
typename TCompactMap<TKey, TValue, N, TCompare, TAllocator>::size_type
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::count(const key_type& key) const
{
    return FindImpl(this, key) == EndImpl(this) ? 0 : 1;
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
bool TCompactMap<TKey, TValue, N, TCompare, TAllocator>::contains(const key_type& key) const
{
    return FindImpl(this, key) != EndImpl(this);
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::find(const key_type& key) -> iterator
{
    return FindImpl(this, key);
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::find(const key_type& key) const -> const_iterator
{
    return FindImpl(this, key);
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
std::pair<typename TCompactMap<TKey, TValue, N, TCompare, TAllocator>::iterator, bool>
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::insert(const value_type& v)
{
    if (IsSmall()) {
        auto [vectorIt, found] = VectorLowerBound(v.first);

        if (found) {
            return {iterator(vectorIt), false};
        }

        if (Vector_.size() < N) {
            auto inserted = EmplaceSmall(vectorIt, v.first, v.second);
            return {iterator(inserted), true};
        }

        UpgradeToMap();
    }

    auto [mapIt, inserted] = Map_.emplace(v.first, v.second);
    return {iterator(mapIt), inserted};
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
template <class... TArgs>
std::pair<typename TCompactMap<TKey, TValue, N, TCompare, TAllocator>::iterator, bool>
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::emplace(TArgs&&... args)
{
    if (IsSmall()) {
        TVectorValue value(std::forward<TArgs>(args)...);
        auto [vectorIt, found] = VectorLowerBound(value.first);

        if (found) {
            return {iterator(vectorIt), false};
        }

        if (Vector_.size() < N) {
            auto inserted = EmplaceSmall(vectorIt, std::move(value));
            return {iterator(inserted), true};
        }

        UpgradeToMap();

        // We already constructed the value above, so we cannot forward args again below.
        auto [mapIt, inserted] = Map_.emplace(std::move(value));
        return {iterator(mapIt), inserted};
    }

    auto [mapIt, inserted] = Map_.emplace(std::forward<TArgs>(args)...);
    return {iterator(mapIt), inserted};
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
template <class... TArgs>
std::pair<typename TCompactMap<TKey, TValue, N, TCompare, TAllocator>::iterator, bool>
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::try_emplace(const key_type& key, TArgs&&... args)
{
    return TryEmplaceImpl(key, std::forward<TArgs>(args)...);
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
template <class... TArgs>
std::pair<typename TCompactMap<TKey, TValue, N, TCompare, TAllocator>::iterator, bool>
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::try_emplace(key_type&& key, TArgs&&... args)
{
    return TryEmplaceImpl(std::move(key), std::forward<TArgs>(args)...);
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
template <class TIt>
void TCompactMap<TKey, TValue, N, TCompare, TAllocator>::insert(TIt first, TIt last)
{
    for (; first != last; ++first) {
        insert(*first);
    }
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
template <class M>
std::pair<typename TCompactMap<TKey, TValue, N, TCompare, TAllocator>::iterator, bool> TCompactMap<TKey, TValue, N, TCompare, TAllocator>::insert_or_assign(const key_type& key, M&& obj)
{
    if (IsSmall()) {
        auto [vectorIt, found] = VectorLowerBound(key);
        
        if (found) {
            vectorIt->second = std::forward<M>(obj);
            return {iterator(vectorIt), false};
        }

        if (Vector_.size() < N) {
            auto inserted = EmplaceSmall(vectorIt, key, std::forward<M>(obj));
            return {iterator(inserted), true};
        }

        UpgradeToMap();
    }

    auto [mapIt, inserted] = Map_.insert_or_assign(key, std::forward<M>(obj));
    return {iterator(mapIt), inserted};
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
template <class TKeyParam, class... TArgs>
std::pair<typename TCompactMap<TKey, TValue, N, TCompare, TAllocator>::iterator, bool>
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::TryEmplaceImpl(TKeyParam&& key, TArgs&&... args)
{
    if (IsSmall()) {
        auto [vectorIt, found] = VectorLowerBound(key);

        if (found) {
            return {iterator(vectorIt), false};
        }

        if (Vector_.size() < N) {
            auto inserted = EmplaceSmall(
                vectorIt,
                std::piecewise_construct,
                std::forward_as_tuple(std::forward<TKeyParam>(key)),
                std::forward_as_tuple(std::forward<TArgs>(args)...));
            return {iterator(inserted), true};
        }

        UpgradeToMap();
    }

    auto [mapIt, inserted] = Map_.try_emplace(std::forward<TKeyParam>(key), std::forward<TArgs>(args)...);
    return {iterator(mapIt), inserted};
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::erase(const key_type& key) -> size_type
{
    if (IsSmall()) {
        auto [vectorIt, found] = VectorLowerBound(key);

        if (!found) {
            return 0;
        }

        Vector_.erase(vectorIt);
        return 1;
    }

    return Map_.erase(key);
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::erase(iterator pos) -> iterator
{
    return erase(const_iterator(pos));
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::erase(const_iterator pos) -> iterator
{
    if (IsSmall()) {
        return iterator(Vector_.erase(pos.VIter));
    }

    return iterator(Map_.erase(pos.MIter));
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::erase(const_iterator first, const_iterator last) -> iterator
{
    if (IsSmall()) {
        return iterator(Vector_.erase(first.VIter, last.VIter));
    }

    return iterator(Map_.erase(first.MIter, last.MIter));
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
typename TCompactMap<TKey, TValue, N, TCompare, TAllocator>::mapped_type&
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::operator[](const key_type& key)
{
    return Subscript(key);
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
typename TCompactMap<TKey, TValue, N, TCompare, TAllocator>::mapped_type&
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::operator[](key_type&& key)
{
    return Subscript(std::move(key));
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
typename TCompactMap<TKey, TValue, N, TCompare, TAllocator>::mapped_type&
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::at(const key_type& key)
{
    return AtImpl(this, key);
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
const typename TCompactMap<TKey, TValue, N, TCompare, TAllocator>::mapped_type&
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::at(const key_type& key) const
{
    return AtImpl(this, key);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
