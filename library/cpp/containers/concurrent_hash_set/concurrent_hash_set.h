#pragma once

#include <array>
#include <atomic>

#include <library/cpp/yt/threading/writer_starving_rw_spin_lock.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/yexception.h>

// @brief hash_set optimized for read/update from multiple threads
// @note for better performace use a power of two for BucketCount
template <class V, class HashFcn = THash<V>, class EqualKey = TEqualTo<V>, size_t BucketCount = 64>
class TConcurrentHashSet {
    // @brief helper class to avoid double hash calculation for key
    template <class T>
    class TKeyHash {
        using TValue = std::decay_t<T>;

    public:
        template <class U>
        explicit TKeyHash(U&& object)
            : Value(std::forward<U>(object))
            , Hash(HashFcn()(Value))
        {
        }
        template <class U>
        explicit TKeyHash(const U&& object)
            : Value(std::forward<U>(object))
            , Hash(HashFcn()(Value))
        {
        }
        size_t GetHash() const {
            return Hash;
        }

        operator TValue &&() && {
            return std::move(Value);
        }

        const TValue& GetValue() const {
            return Value;
        }

    private:
        TValue Value;
        size_t Hash;
    };

    struct THasher : HashFcn {
        template <class Value>
        size_t operator()(const TKeyHash<Value>& kh) const {
            return kh.GetHash();
        }

        using HashFcn::operator();
    };

    struct TEqualer : EqualKey {
        template <class T>
        bool operator()(const TKeyHash<T>& l, const TKeyHash<T>& r) {
            return EqualKey::operator()(l.GetValue(), r.GetValue());
        }

        template <class X, class Y>
        bool operator()(const TKeyHash<X>& l, const Y& r) {
            return EqualKey::operator()(l.GetValue(), r);
        }

        template <class X, class Y>
        bool operator()(const X& l, const TKeyHash<Y>& r) {
            return EqualKey::operator()(l, r.GetValue());
        }

        using EqualKey::operator();
    };

    using TLock = NYT::NThreading::TWriterStarvingRWSpinLock;
    using TReaderGuard = NYT::NThreading::TReaderGuard<TLock>;
    using TWriterGuard = NYT::NThreading::TWriterGuard<TLock>;
    using TActualSet = THashSet<V, THasher, TEqualer>;

    class TBucket {
        TActualSet Set;
        mutable TLock Lock;

    public:
        // all methods are return copy of item, since it is single way met thread-safety requirements

        // @brief synchronously get item from set
        // @raise exception if there is no item with key
        template <class T>
        V Get(TKeyHash<T>&& value) const {
            TReaderGuard guard(Lock);
            auto it = Set.find(std::move(value));
            Y_ENSURE(it != Set.end(), "not found by value");
            return *it;
        }

        // @brief try to get item by key.
        // @return nothing if there is no item
        template <class T>
        TMaybe<V> Find(TKeyHash<T>&& value) const {
            TReaderGuard guard(Lock);
            auto it = Set.find(std::move(value));
            if (Set.end() != it) {
                return MakeMaybe(*it);
            }
            return Nothing();
        }

        // @brief synchronously checks that there is item with key in the set
        template <class T>
        bool Contains(TKeyHash<T>&& value) const {
            TReaderGuard guard(Lock);
            return Set.contains(value);
        }

        // @brief synchronously try to insert new item to set
        // @return true if item was inserted otherwise false
        template <class T>
        bool Insert(TKeyHash<T>&& value) {
            TWriterGuard guard(Lock);
            return Set.emplace(std::move(value)).second;
        }

        // @brief synchronously insert new item or update existing
        // @return true if item was inserted ortherwise false
        template <class T>
        bool InsertReplace(TKeyHash<T>&& value) {
            TWriterGuard guard(Lock);
            typename TActualSet::insert_ctx ctx;
            auto it = Set.find(value, ctx);
            bool inserted = true;
            if (Set.end() != it) {
                Set.erase(it);
                inserted = false;
            }
            Set.emplace_direct(ctx, std::move(value));
            return inserted;
        }

        // @brief synchronously removes item from set, returns true if item was removed otherwise false
        template <class T>
        bool Erase(TKeyHash<T>&& value) {
            TWriterGuard guard(Lock);
            auto it = Set.find(std::move(value));
            if (Set.end() != it) {
                Set.erase(it);
                return true;
            }
            return false;
        }
    };

private:
    size_t GetHashForBucket(const size_t& value) const {
        return value % BucketCount;
    }

    TBucket& GetBucketForValue(const size_t& value) {
        return Buckets[GetHashForBucket(value)];
    }

    const TBucket& GetBucketForValue(const size_t& value) const {
        return Buckets[GetHashForBucket(value)];
    }

public:
    TConcurrentHashSet()
        : SizeSet{0} {
    }

    // @brief try to insert new item to set
    // @return true on success, false if there is alredy item with same key
    template <typename TheKey>
    bool Insert(TheKey&& value) {
        TKeyHash<TheKey> kh(std::forward<TheKey>(value));
        if (GetBucketForValue(kh.GetHash()).Insert(std::move(kh))) {
            SizeSet.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
        return false;
    }

    // @brief update existing or insert new (it is effective analog for find, erase, insert)
    // @retrun true if item was inserted and false if updated
    template <typename TheKey>
    bool InsertReplace(TheKey&& value) {
        TKeyHash<TheKey> kh(std::forward<TheKey>(value));
        if (GetBucketForValue(kh.GetHash()).InsertReplace(std::move(kh))) {
            SizeSet.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
        return false;
    }

    // @brief erase item by key
    template <class TheKey>
    bool Erase(TheKey&& value) {
        TKeyHash<TheKey> kh(std::forward<TheKey>(value));
        if (GetBucketForValue(kh.GetHash()).Erase(std::move(kh))) {
            SizeSet.fetch_sub(1, std::memory_order_relaxed);
            return true;
        }
        return false;
    }

    // @brief get item by key
    // @raise exception if there is no item for key
    // @return copy of item
    template <class TheKey>
    V Get(TheKey&& value) const {
        TKeyHash<TheKey> kh(std::forward<TheKey>(value));
        return GetBucketForValue(kh.GetHash()).Get(std::move(kh));
    }

    // @brief try to get item by key
    // @return copy of item or noting if there is no item
    template <class TheKey>
    TMaybe<V> Find(TheKey&& value) const {
        TKeyHash<TheKey> kh(std::forward<TheKey>(value));
        return GetBucketForValue(kh.GetHash()).Find(std::move(kh));
    }

    // @brief check that there is item in set
    template <class TheKey>
    bool Contains(TheKey&& value) const {
        TKeyHash<TheKey> kh(std::forward<TheKey>(value));
        return GetBucketForValue(kh.GetHash()).Contains(std::move(kh));
    }

    // @brief returns number of elements
    size_t Size() const {
        return SizeSet.load(std::memory_order_relaxed);
    }

private:
    std::atomic_size_t SizeSet;
    std::array<TBucket, BucketCount> Buckets;
};
