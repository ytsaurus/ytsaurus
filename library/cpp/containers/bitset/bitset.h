#pragma once

#include <util/generic/bitmap.h>
#include <util/generic/ylimits.h>

#include <iterator>

//! Drop-in replacement for THashSet<int> when range of values is small < 10K.
//! All items in range should be less than Max<i32>.
template <typename T>
class TBitSet;

template <>
class TBitSet<i32>
{
public:
    class TIterator
        : public std::iterator<std::input_iterator_tag, i32>
    {
    public:
        bool operator!=(const TIterator& other) const
        {
            return Pos_ != other.Pos_;
        }
        bool operator==(const TIterator& other) const
        {
            return Pos_ == other.Pos_;
        }

        i32 operator*() const
        {
            return static_cast<i32>(Pos_) + BitSet_->Offset_;
        }

        TIterator& operator++()
        {
            Pos_ = BitSet_->BitMap_.NextNonZeroBit(Pos_);
            return *this;
        }
        TIterator operator++(int)
        {
            auto result = *this;
            ++(*this);
            return result;
        }

    protected:
        friend class TBitSet<i32>;

        TIterator(const TBitSet<i32>* bitSet, size_t pos)
            : BitSet_(bitSet)
            , Pos_(pos)
        { }

        const TBitSet<i32>* BitSet_;
        size_t Pos_;
    };

    using iterator = TIterator;

    //! Construct an empty bitset.
    TBitSet() = default;

    //! Construct en empty bitset and reserves memory to insert at least size elements w/o allocation.
    explicit TBitSet(size_t size)
    {
        BitMap_.Reserve(size);
    }

    //! Construct bitset which will contain unique element from items.
    //! It would be better if items will be sorted.
    TBitSet(std::initializer_list<i32> items)
    {
        insert(items);
    }

    //! Construct bitset which will contain unique element from range [first, last).
    //! It would be better if items will be sorted.
    template <typename T>
    TBitSet(const T& first, const T& last)
    {
        insert(first, last);
    }

    //! Make bitset empty, but do not return allocated memory.
    void clear()
    {
        BitMap_.Clear();
    }

    //! Checks that key is presented in bitset.
    bool contains(i32 key) const
    {
        return BitMap_.Test(static_cast<ui32>(key - Offset_));
    }

    //! Return 1 if key in bitset, otherwise 0.
    size_t count(i32 key) const
    {
        return contains(key);
    }

    //! Find item by key.
    //! Returns iterator to item or end() if item does not found.
    iterator find(i32 key) const
    {
        return contains(key) ? iterator(this, static_cast<ui32>(key - Offset_)) : end();
    }

    //! Count of elements.
    size_t size() const
    {
        return BitMap_.Count();
    }

    //! Returns the current size of storage.
    size_t capacity() const
    {
        return BitMap_.Size();
    }

    //! Returns true if bitset contains no items.
    bool empty() const
    {
        return BitMap_.Empty();
    }

    operator bool() const
    {
        return !empty();
    }

    void swap(TBitSet& other)
    {
        std::swap(Offset_, other.Offset_);
        BitMap_.Swap(other.BitMap_);
    }

    //! Remove item with equal key if it was in bitset.
    void erase(i32 key)
    {
        BitMap_.Reset(static_cast<ui32>(key - Offset_));
    }

    //! Remove item which is pointed by iterator.
    void erase(const iterator& it)
    {
        erase(*it);
    }

    //! Remove all items from range [first, last).
    void erase(const iterator& first, const iterator& last)
    {
        for (auto it = first; it != last; ++it) {
            erase(*it);
        }
    }

    iterator begin() const
    {
        return iterator(this, BitMap_.FirstNonZeroBit());
    }
    iterator end() const
    {
        return iterator(this, BitMap_.Size());
    }

    //! Try to add new item to bitset.
    //! Returns iterator and true if element was inserted, false if it already exists.
    std::pair<iterator, bool> emplace(i32 key);
    std::pair<iterator, bool> insert(const i32 key)
    {
        return emplace(key);
    }

    //! Insert all items from range [first, last).
    template <typename T>
    void insert(T first, T last)
    {
        for (auto it = first; it != last; ++it) {
            emplace(*it);
        }
    }
    template <typename T>
    void insert(std::initializer_list<T> items) {
        insert(items.begin(), items.end());
    }

    TBitSet& operator|=(const TBitSet& other);
    TBitSet& operator&=(const TBitSet& other);
    TBitSet& operator^=(const TBitSet& other);

private:
    i32 Offset_ = Max<i32>();
    TDynBitMap BitMap_;
};

template <typename T>
class TBitSet
    : private TBitSet<i32>
{
    static_assert(sizeof(T) <= sizeof(i32));

    using TBase = TBitSet<i32>;
public:
    class TIterator
        : public TBitSet<i32>::TIterator
    {
    public:
        T operator*() const
        {
            return static_cast<T>(TBitSet<i32>::TIterator::operator*());
        }
    private:
        TIterator(TBitSet<i32>::TIterator base)
            : TBitSet<i32>::TIterator(std::move(base))
        { }

        friend class TBitSet<T>;
    };

    using iterator = TIterator;

    using TBase::TBitSet;
    using TBase::clear;
    using TBase::size;
    using TBase::capacity;
    using TBase::empty;
    using TBase::swap;
    using TBase::operator bool;
    using TBase::erase;
    using TBase::insert;

    TBitSet(std::initializer_list<T> items)
    {
        insert(items);
    }

    bool contains(T key) const
    {
        return TBase::contains(static_cast<i32>(key));
    }
    size_t count(T key) const
    {
        return TBase::count(static_cast<i32>(key));
    }

    iterator find(T key) const
    {
        return iterator(TBase::find(static_cast<i32>(key)));
    }

    void erase(T key)
    {
        TBase::erase(static_cast<i32>(key));
    }

    iterator begin() const
    {
        return iterator(TBase::begin());
    }

    iterator end() const
    {
        return iterator(TBase::end());
    }

    std::pair<iterator, bool> insert(T key)
    {
        return emplace(key);
    }
    std::pair<iterator, bool> emplace(T key)
    {
        auto itb = TBase::emplace(static_cast<i32>(key));
        return {iterator(itb.first), itb.second};
    }
    void insert(std::initializer_list<T> items)
    {
        TBase::insert(items.begin(), items.end());
    }

    TBitSet& operator|=(const TBitSet& other)
    {
        return static_cast<TBitSet&>(static_cast<TBase&>(*this) |= other);
    }

    TBitSet& operator&=(const TBitSet& other)
    {
        return static_cast<TBitSet&>(static_cast<TBase&>(*this) &= other);
    }

    TBitSet& operator^=(const TBitSet& other)
    {
        return static_cast<TBitSet&>(static_cast<TBase&>(*this) ^= other);
    }
};

template <typename T>
TBitSet<T> operator|(const TBitSet<T>& left, const TBitSet<T>& right)
{
    TBitSet<T> tmp = left;
    tmp |= right;
    return tmp;
}

template <typename T>
TBitSet<T> operator&(const TBitSet<T>& left, const TBitSet<T>& right)
{
    TBitSet<T> tmp = left;
    tmp &= right;
    return tmp;
}

template <typename T>
TBitSet<T> operator^(const TBitSet<T>& left, const TBitSet<T>& right)
{
    TBitSet<T> tmp = left;
    tmp ^= right;
    return tmp;
}
