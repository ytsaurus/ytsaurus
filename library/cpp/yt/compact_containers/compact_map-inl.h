#ifndef COMPACT_MAP_INL_H_
#error "Direct inclusion of this file is not allowed, include compact_map.h"
// For the sake of sane code completion.
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

    using TVecIter = std::conditional_t<IsConst, TArrayConstIterator, TArrayIterator>;
    using TMapIter = std::conditional_t<IsConst, TMapConstIterator, TMapIterator>;
    using TValueType = typename TCompactMap::value_type;
    using TArrayValue = typename TCompactMap::TArrayValue;
    using TSmallValue = std::conditional_t<IsConst, const TArrayValue, TArrayValue>;
    using TSmallPointer = std::conditional_t<IsConst, const TArrayValue*, TArrayValue*>;
    using TReference = std::conditional_t<IsConst, const TValueType&, TValueType&>;
    using TPointer = std::conditional_t<IsConst, const TValueType*, TValueType*>;

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
            return *VIter;
        }
        return *MIter;
    }

    pointer operator->() const
    {
        if (Small_) {
            return &*VIter;
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

    explicit iterator(TArrayIterator it)
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

    explicit const_iterator(TArrayConstIterator it)
        : TBase(it)
    { }

    explicit const_iterator(TMapConstIterator it)
        : TBase(it)
    { }
};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::ArrayData() -> TArrayIterator
{
    static_assert(sizeof(TStorage) == sizeof(TArrayValue), "Inline storage size mismatch");
    static_assert(alignof(TStorage) == alignof(TArrayValue), "Inline storage alignment mismatch");
    return reinterpret_cast<TArrayIterator>(ArrayStorage_.data());
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::ArrayData() const -> TArrayConstIterator
{
    return reinterpret_cast<TArrayConstIterator>(ArrayStorage_.data());
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::ArrayBegin() -> TArrayIterator
{
    return ArrayData();
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::ArrayBegin() const -> TArrayConstIterator
{
    return ArrayData();
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::ArrayEnd() -> TArrayIterator
{
    return ArrayData() + ArraySize_;
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::ArrayEnd() const -> TArrayConstIterator
{
    return ArrayData() + ArraySize_;
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
void TCompactMap<TKey, TValue, N, TCompare, TAllocator>::ClearArray()
{
    auto* data = ArrayData();
    for (size_type index = 0; index < ArraySize_; ++index) {
        std::destroy_at(data + index);
    }
    ArraySize_ = 0;
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
void TCompactMap<TKey, TValue, N, TCompare, TAllocator>::CopyArrayFrom(const TCompactMap& other)
{
    auto* data = ArrayData();
    auto* otherData = other.ArrayData();

    for (size_type index = 0; index < other.ArraySize_; ++index) {
        std::construct_at(data + index, otherData[index]);
    }
    ArraySize_ = other.ArraySize_;
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
void TCompactMap<TKey, TValue, N, TCompare, TAllocator>::MoveArrayFrom(TCompactMap&& other)
{
    auto* data = ArrayData();
    auto* otherData = other.ArrayData();

    for (size_type index = 0; index < other.ArraySize_; ++index) {
        std::construct_at(data + index, std::move(otherData[index]));
    }
    ArraySize_ = other.ArraySize_;

    other.ClearArray();
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::TCompactMap(const TCompactMap& other)
    : Compare_(other.Compare_)
    , Alloc_(other.Alloc_)
{
    if (other.IsSmall()) {
        CopyArrayFrom(other);
    } else {
        Map_ = other.Map_;
    }
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::TCompactMap(TCompactMap&& other) noexcept(
    std::is_nothrow_move_constructible_v<value_type> &&
    std::is_nothrow_move_constructible_v<key_compare> &&
    std::allocator_traits<allocator_type>::is_always_equal::value)
    : Compare_(std::move(other.Compare_))
    , Alloc_(std::move(other.Alloc_))
{
    if (other.IsSmall()) {
        MoveArrayFrom(std::move(other));
    } else {
        Map_ = std::move(other.Map_);
    }
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
TCompactMap<TKey, TValue, N, TCompare, TAllocator>&
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::operator=(const TCompactMap& other)
{
    if (this == &other) {
        return *this;
    }

    ClearArray();
    Map_.clear();

    Compare_ = other.Compare_;
    Alloc_ = other.Alloc_;

    if (other.IsSmall()) {
        CopyArrayFrom(other);
    } else {
        Map_ = other.Map_;
    }

    return *this;
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
TCompactMap<TKey, TValue, N, TCompare, TAllocator>&
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::operator=(TCompactMap&& other) noexcept(
    std::is_nothrow_move_constructible_v<value_type> &&
    std::is_nothrow_move_constructible_v<key_compare> &&
    std::allocator_traits<allocator_type>::is_always_equal::value)
{
    if (this == &other) {
        return *this;
    }

    ClearArray();
    Map_.clear();

    Compare_ = std::move(other.Compare_);
    Alloc_ = std::move(other.Alloc_);

    if (other.IsSmall()) {
        MoveArrayFrom(std::move(other));
    } else {
        Map_ = std::move(other.Map_);
    }

    return *this;
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::~TCompactMap()
{
    ClearArray();
}

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
    return IsSmall() ? ArraySize_ == 0 : Map_.empty();
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
typename TCompactMap<TKey, TValue, N, TCompare, TAllocator>::size_type
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::size() const
{
    return IsSmall() ? ArraySize_ : Map_.size();
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
void TCompactMap<TKey, TValue, N, TCompare, TAllocator>::clear()
{
    ClearArray();
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

    TMap newMap(Compare_, Alloc_);
    auto* data = ArrayData();
    for (size_type index = 0; index < ArraySize_; ++index) {
        newMap.emplace(data[index].first, std::move(data[index].second));
    }

    ClearArray();
    Map_ = std::move(newMap);
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
std::pair<typename TCompactMap<TKey, TValue, N, TCompare, TAllocator>::TArrayConstIterator, bool>
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::ArrayLowerBound(const key_type& key) const
{
    return ArrayLowerBoundImpl(ArrayBegin(), ArrayEnd(), key, Compare_);
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
std::pair<typename TCompactMap<TKey, TValue, N, TCompare, TAllocator>::TArrayIterator, bool>
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::ArrayLowerBound(const key_type& key)
{
    return ArrayLowerBoundImpl(ArrayBegin(), ArrayEnd(), key, Compare_);
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
template <class... TArgs>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::EmplaceSmall(TArrayConstIterator pos, TArgs&&... args) -> TArrayIterator
{
    auto index = static_cast<size_type>(pos - ArrayBegin());
    auto* data = ArrayData();

    // Simple path: emplace at the end.
    if (index == ArraySize_) {
        std::construct_at(data + ArraySize_, std::forward<TArgs>(args)...);
        ++ArraySize_;
        return data + index;
    }

    // Shift elements to make space at index.
    std::construct_at(data + ArraySize_, std::move(data[ArraySize_ - 1]));
    for (size_type i = ArraySize_ - 1; i > index; --i) {
        std::destroy_at(data + i);
        std::construct_at(data + i, std::move(data[i - 1]));
    }

    // Construct new element at index.
    std::destroy_at(data + index);
    std::construct_at(data + index, std::forward<TArgs>(args)...);

    ++ArraySize_;
    return data + index;
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::EraseSmall(TArrayConstIterator pos) -> TArrayIterator
{
    return EraseSmall(pos, pos + 1);
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::EraseSmall(TArrayConstIterator first, TArrayConstIterator last) -> TArrayIterator
{
    auto start = static_cast<size_type>(first - ArrayBegin());
    auto finish = static_cast<size_type>(last - ArrayBegin());
    auto* data = ArrayData();

    if (start == finish) {
        return data + start;
    }

    // Destroy elements in [start, finish).
    for (size_type index = start; index < finish; ++index) {
        std::destroy_at(data + index);
    }

    // Shift elements from [finish, ArraySize_) to [start, ...).
    for (size_type index = finish; index < ArraySize_; ++index) {
        std::construct_at(data + start + (index - finish), std::move(data[index]));
        std::destroy_at(data + index);
    }

    ArraySize_ -= (finish - start);
    return data + start;
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
template <class TArrayIteratorType>
std::pair<TArrayIteratorType, bool>
TCompactMap<TKey, TValue, N, TCompare, TAllocator>::ArrayLowerBoundImpl(
    TArrayIteratorType begin,
    TArrayIteratorType end,
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
        auto [arrayIt, found] = ArrayLowerBound(key);

        if (found) {
            return arrayIt->second;
        }

        if (ArraySize_ < N) {
            auto inserted = EmplaceSmall(arrayIt, std::forward<TKeyParam>(key), mapped_type());
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
        return TResult(self->ArrayBegin());
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
        return TResult(self->ArrayEnd());
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
        auto [it, found] = self->ArrayLowerBound(key);
        return found ? TResult(it) : TResult(self->ArrayEnd());
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
        auto [arrayIt, found] = ArrayLowerBound(v.first);

        if (found) {
            return {iterator(arrayIt), false};
        }

        if (ArraySize_ < N) {
            auto inserted = EmplaceSmall(arrayIt, v.first, v.second);
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
        TArrayValue value(std::forward<TArgs>(args)...);
        auto [arrayIt, found] = ArrayLowerBound(value.first);

        if (found) {
            return {iterator(arrayIt), false};
        }

        if (ArraySize_ < N) {
            auto inserted = EmplaceSmall(arrayIt, std::move(value));
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
        auto [arrayIt, found] = ArrayLowerBound(key);
        
        if (found) {
            arrayIt->second = std::forward<M>(obj);
            return {iterator(arrayIt), false};
        }

        if (ArraySize_ < N) {
            auto inserted = EmplaceSmall(arrayIt, key, std::forward<M>(obj));
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
        auto [arrayIt, found] = ArrayLowerBound(key);

        if (found) {
            return {iterator(arrayIt), false};
        }

        if (ArraySize_ < N) {
            auto inserted = EmplaceSmall(
                arrayIt,
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
        auto [arrayIt, found] = ArrayLowerBound(key);

        if (!found) {
            return 0;
        }

        EraseSmall(arrayIt);
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
        return iterator(EraseSmall(pos.VIter));
    }

    return iterator(Map_.erase(pos.MIter));
}

template <class TKey, class TValue, size_t N, class TCompare, class TAllocator>
auto TCompactMap<TKey, TValue, N, TCompare, TAllocator>::erase(const_iterator first, const_iterator last) -> iterator
{
    if (IsSmall()) {
        return iterator(EraseSmall(first.VIter, last.VIter));
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
