#pragma once

#include "static_vector.h"

namespace NLockFreeMap::NPrivate {

    template <class _TKeyHash, class _TKeyCompare, class _TKeySelect, class _TReclaimer, class _TBackoff, std::size_t _NumOfBuckets>
    struct THarrisMichaelHashTablePolicy {
        using TKeyHash = _TKeyHash;
        using TKeyCompare = _TKeyCompare;
        using TKeySelect = _TKeySelect;
        using TReclaimer = _TReclaimer;
        using TBackoff = _TBackoff;
        static constexpr auto NumOfBuckets = _NumOfBuckets;
    };

    template <class TValue, class TPolicy>
    class THarrisMichaelHashTable {
    private:
        using TKeyHash = typename TPolicy::TKeyHash;
        using TKeyCompare = typename TPolicy::TKeyCompare;
        using TKeySelect = typename TPolicy::TKeySelect;
        using TReclaimer = typename TPolicy::TReclaimer;
        using TBackoff = typename TPolicy::TBackoff;
        static constexpr auto NumOfBuckets = TPolicy::NumOfBuckets;

        template <bool IsConst>
        class TIterator;

        struct TBucket;
        using TBuckets = TStaticVector<TBucket, NumOfBuckets>;

        using TKey = typename TKeySelect::TType;

    public:
        using value_type = TValue;
        using key_type = TKey;

        using hasher = TKeyHash;

        using iterator = TIterator<!std::is_same_v<TValue, TKey>>;
        using const_iterator = TIterator<true>;

        using accessor = typename TBucket::accessor;
        using const_accessor = typename TBucket::const_accessor;

        using key_select = TKeySelect;

        explicit THarrisMichaelHashTable(const TKeyHash& hasher = {}, const TKeyCompare& compare = {}, const TReclaimer& reclaimer = {}) noexcept;

        THarrisMichaelHashTable(const THarrisMichaelHashTable&) = delete;
        THarrisMichaelHashTable& operator=(const THarrisMichaelHashTable&) = delete;

        bool insert(const TValue& value);
        bool insert(TValue&& value);

        template <class _TValue>
        requires std::constructible_from<TValue, _TValue&&>
        bool insert(_TValue&& value);

        template <class... TArgs>
        requires std::constructible_from<TValue, TArgs&&...>
        bool emplace(TArgs&&... args);

        bool erase(const TKey& key);
        accessor extract(const TKey& key);

        accessor at(const TKey& key);
        const_accessor at(const TKey& key) const;

        iterator find(const TKey& key);
        const_iterator find(const TKey& key) const;

        bool contains(const TKey& key) const;

        void clear();

        std::size_t size() const noexcept;
        bool empty() const noexcept;

        iterator begin();
        iterator end();

        const_iterator cbegin() const;
        const_iterator cend() const;

        const_iterator begin() const;
        const_iterator end() const;

    private:
        static std::size_t GetBucketIdx(std::size_t hash) noexcept;

        TBucket& GetBucket(const TKey& key) noexcept;
        const TBucket& GetBucket(const TKey& key) const noexcept;

    private:
        TBuckets Buckets_{};
        Y_NO_UNIQUE_ADDRESS TKeyHash KeyHash_{};
        Y_NO_UNIQUE_ADDRESS TKeySelect KeySelect_{};
    };

    template <class TValue, std::size_t NumOfBuckets, class... TOptions>
    struct TMakeHarrisMichaelSet;

    template <class TKey, class TValue, std::size_t NumOfBuckets, class... TOptions>
    struct TMakeHarrisMichaelMap;

} // namespace NLockFreeMap::NPrivate

namespace NLockFreeMap {

    template <class TValue, std::size_t NumOfBuckets, class... TOptions>
    using THarrisMichaelSet = typename NPrivate::TMakeHarrisMichaelSet<TValue, NumOfBuckets, TOptions...>::TType;

    template <class TKey, class TValue, std::size_t NumOfBuckets, class... TOptions>
    using THarrisMichaelMap = typename NPrivate::TMakeHarrisMichaelMap<TKey, TValue, NumOfBuckets, TOptions...>::TType;

} // namespace NLockFreeMap

#define INCLUDE_HARRIS_MICHAEL_HASHTABLE_INL_H
#include "harris_michael_hashtable-inl.h"
#undef INCLUDE_HARRIS_MICHAEL_HASHTABLE_INL_H
