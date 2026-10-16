#ifndef INCLUDE_HARRIS_MICHAEL_HASHTABLE_INL_H
    #error "include harris_michael_hashtable.h instead"
    #include "harris_michael_hashtable.h"
#endif

#include "harris_michael_list.h"

#include <utility>

namespace NLockFreeMap::NPrivate {

    // ============================================================================================
    // THarrisMichaelHashTable
    // ============================================================================================

    template <class TValue, class TPolicy>
    struct THarrisMichaelHashTable<TValue, TPolicy>::TBucket
        : public THarrisMichaelList<TValue, THarrisMichaelListPolicy<TKeyCompare, TKeySelect, TReclaimer, TBackoff>> {
        using TBase = THarrisMichaelList<TValue, THarrisMichaelListPolicy<TKeyCompare, TKeySelect, TReclaimer, TBackoff>>;
        using TNode = typename TBase::TNode;

        using TBase::InsertNode;
        using TBase::TBase;
    };

    template <class TValue, class TPolicy>
    template <bool IsConst>
    class THarrisMichaelHashTable<TValue, TPolicy>::TIterator {
        friend class THarrisMichaelHashTable<TValue, TPolicy>;

        class TDummyNonConstIter;
        using TNonConstIter = std::conditional_t<IsConst, TIterator<false>, TDummyNonConstIter>;

        using TListIt = std::conditional_t<IsConst, typename TBucket::const_iterator, typename TBucket::iterator>;
        using TBucketsIt = std::conditional_t<IsConst, typename TBuckets::const_iterator, typename TBuckets::iterator>;

    public:
        using value_type = typename TListIt::value_type;
        using difference_type = typename TListIt::difference_type;
        using pointer = typename TListIt::pointer;
        using reference = typename TListIt::reference;
        using iterator_category = std::forward_iterator_tag;

        TIterator() = default;

        TIterator(TBucketsIt beg, TBucketsIt end, TListIt cur)
            : Current_(std::move(cur))
            , BucketsBeg_(beg)
            , BucketsEnd_(end)
        {
            if (Current_ == BucketsBeg_->end()) {
                Increment();
            }
        }

        TIterator(const TIterator& other) = default;
        TIterator& operator=(const TIterator& other) = default;

        TIterator(const TNonConstIter& other) noexcept
            : Current_(other.Current_)
            , BucketsBeg_(other.BucketBeg_)
            , BucketsEnd_(other.BucketEnd_)
        {}

        TIterator& operator=(const TNonConstIter& other) noexcept {
            TIterator{other}.Swap(*this);
            return *this;
        }

        TIterator(TIterator&& other) = default;
        TIterator& operator=(TIterator&& other) = default;

        TIterator(TNonConstIter&& other) noexcept
            : Current_(std::move(other.Current_))
            , BucketsBeg_(other.BucketBeg_)
            , BucketsEnd_(other.BucketEnd_)
        {}

        TIterator& operator=(TNonConstIter&& other) noexcept {
            TIterator{std::move(other)}.Swap(*this);
            return *this;
        }

        TIterator& operator++() {
            Increment();
            return *this;
        }

        TIterator operator++(int) {
            auto copy = TIterator{*this};
            Increment();
            return copy;
        }

        reference operator*() const noexcept {
            return *Current_;
        }

        pointer operator->() const noexcept {
            return std::addressof(this->operator*());
        }

        void Swap(TIterator& other) noexcept {
            std::ranges::swap(Current_, other.Current_);
            std::ranges::swap(BucketsBeg_, other.BucketsBeg_);
            std::ranges::swap(BucketsEnd_, other.BucketsEnd_);
        }

        friend void swap(TIterator& left, TIterator& right) noexcept { // NOLINT(readability-identifier-naming)
            left.Swap(right);
        }

        friend bool operator==(const TIterator& left, const TIterator& right) noexcept {
            return left.Current_ == right.Current_;
        }

    private:
        void Increment() {
            if (BucketsBeg_ != BucketsEnd_) {
                if (++Current_ != BucketsBeg_->end()) {
                    return;
                }
                while (++BucketsBeg_ != BucketsEnd_) {
                    Current_ = BucketsBeg_->begin();
                    if (Current_ != BucketsBeg_->end()) {
                        return;
                    }
                }
            }
            Current_ = TListIt{};
            BucketsBeg_ = TBucketsIt{};
            BucketsEnd_ = TBucketsIt{};
        }

    private:
        TListIt Current_{};
        TBucketsIt BucketsBeg_{};
        TBucketsIt BucketsEnd_{};
    };

    template <class TValue, class TPolicy>
    THarrisMichaelHashTable<TValue, TPolicy>::THarrisMichaelHashTable(const TKeyHash& hasher, const TKeyCompare& compare, const TReclaimer& reclaimer) noexcept
        : KeyHash_(hasher)
    {
        for (auto i = 0ul; i < NumOfBuckets; ++i) {
            Buckets_.EmplaceBack(compare, reclaimer);
        }
    }

    template <class TValue, class TPolicy>
    bool THarrisMichaelHashTable<TValue, TPolicy>::insert(const TValue& value) {
        return emplace(value);
    }

    template <class TValue, class TPolicy>
    bool THarrisMichaelHashTable<TValue, TPolicy>::insert(TValue&& value) {
        return emplace(std::move(value));
    }

    template <class TValue, class TPolicy>
    template <class _TValue>
    requires std::constructible_from<TValue, _TValue&&>
    bool THarrisMichaelHashTable<TValue, TPolicy>::insert(_TValue&& value) {
        return emplace(std::forward<_TValue>(value));
    }

    template <class TValue, class TPolicy>
    template <class... TArgs>
    requires std::constructible_from<TValue, TArgs&&...>
    bool THarrisMichaelHashTable<TValue, TPolicy>::emplace(TArgs&&... args) {
        if constexpr (std::is_invocable_v<TKeySelect, TArgs...>) {
            auto&& key = KeySelect_(args...);
            auto&& bucket = GetBucket(key);
            return bucket.emplace(std::forward<TArgs>(args)...);
        } else {
            auto&& newNode = MakeHolder<typename TBucket::TNode>(std::forward<TArgs>(args)...);
            auto&& bucket = GetBucket(KeySelect_(newNode->Value));
            return bucket.InsertNode(std::move(newNode));
        }
    }

    template <class TValue, class TPolicy>
    bool THarrisMichaelHashTable<TValue, TPolicy>::erase(const TKey& key) {
        return GetBucket(key).erase(key);
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelHashTable<TValue, TPolicy>::extract(const TKey& key) -> accessor {
        return GetBucket(key).extract(key);
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelHashTable<TValue, TPolicy>::at(const TKey& key) -> accessor {
        return GetBucket(key).at(key);
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelHashTable<TValue, TPolicy>::at(const TKey& key) const -> const_accessor {
        return GetBucket(key).at(key);
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelHashTable<TValue, TPolicy>::find(const TKey& key) -> iterator {
        auto&& bucketIdx = GetBucketIdx(KeyHash_(key));
        auto&& bucket = Buckets_[bucketIdx];
        if (auto found = bucket.find(key); found != bucket.end()) {
            return iterator{Buckets_.begin() + bucketIdx, Buckets_.end(), std::move(found)};
        }
        return iterator{};
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelHashTable<TValue, TPolicy>::find(const TKey& key) const -> const_iterator {
        auto&& bucketIdx = GetBucketIdx(KeyHash_(key));
        auto&& bucket = Buckets_[bucketIdx];
        if (auto found = bucket.find(key); found != bucket.end()) {
            return const_iterator{Buckets_.begin() + bucketIdx, Buckets_.end(), std::move(found)};
        }
        return const_iterator{};
    }

    template <class TValue, class TPolicy>
    bool THarrisMichaelHashTable<TValue, TPolicy>::contains(const TKey& key) const {
        return GetBucket(key).contains(key);
    }

    template <class TValue, class TPolicy>
    void THarrisMichaelHashTable<TValue, TPolicy>::clear() {
        for (auto&& bucket : Buckets_) {
            bucket.clear();
        }
    }

    template <class TValue, class TPolicy>
    std::size_t THarrisMichaelHashTable<TValue, TPolicy>::size() const noexcept {
        auto size = 0ul;
        for (auto&& bucket : Buckets_) {
            size += bucket.size();
        }
        return size;
    }

    template <class TValue, class TPolicy>
    bool THarrisMichaelHashTable<TValue, TPolicy>::empty() const noexcept {
        return !size();
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelHashTable<TValue, TPolicy>::begin() -> iterator {
        return iterator{Buckets_.begin(), Buckets_.end(), Buckets_.begin()->begin()};
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelHashTable<TValue, TPolicy>::end() -> iterator {
        return iterator{};
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelHashTable<TValue, TPolicy>::cbegin() const -> const_iterator {
        return const_iterator{Buckets_.begin(), Buckets_.end(), Buckets_.begin()->begin()};
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelHashTable<TValue, TPolicy>::cend() const -> const_iterator {
        return const_iterator{};
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelHashTable<TValue, TPolicy>::begin() const -> const_iterator {
        return cbegin();
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelHashTable<TValue, TPolicy>::end() const -> const_iterator {
        return cend();
    }

    template <class TValue, class TPolicy>
    std::size_t THarrisMichaelHashTable<TValue, TPolicy>::GetBucketIdx(std::size_t hash) noexcept {
        if constexpr ((NumOfBuckets & (NumOfBuckets - 1)) == 0) {
            return hash & (NumOfBuckets - 1);
        } else {
            return hash % NumOfBuckets;
        }
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelHashTable<TValue, TPolicy>::GetBucket(const TKey& key) noexcept -> TBucket& {
        return Buckets_[GetBucketIdx(KeyHash_(key))];
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelHashTable<TValue, TPolicy>::GetBucket(const TKey& key) const noexcept -> const TBucket& {
        return Buckets_[GetBucketIdx(KeyHash_(key))];
    }

    template <class TValue, std::size_t NumOfBuckets, class... TOptions>
    struct TMakeHarrisMichaelSet {
        using THasher = NHp::NOptions::TOptionType<NOptions::THasher, THash<TValue>, TOptions...>;
        using TKeyCompare = NHp::NOptions::TOptionType<NOptions::TCompare, TLess<TValue>, TOptions...>;
        using TBackoff = NHp::NOptions::TOptionType<NOptions::TBackoff, TNoneBackoff, TOptions...>;
        using TReclaimer = NHp::NOptions::TOptionType<NOptions::TReclaimer, THazardPointerReclaimer, TOptions...>;

        using TType = THarrisMichaelHashTable<
            TValue,
            THarrisMichaelHashTablePolicy<THasher, TKeyCompare, TSetKeySelect<TValue>, TReclaimer, TBackoff, NumOfBuckets>>;
    };

    template <class TKey, class TValue, std::size_t NumOfBuckets, class... TOptions>
    struct TMakeHarrisMichaelMap {
        using THasher = NHp::NOptions::TOptionType<NOptions::THasher, THash<TKey>, TOptions...>;
        using TKeyCompare = NHp::NOptions::TOptionType<NOptions::TCompare, TLess<TKey>, TOptions...>;
        using TBackoff = NHp::NOptions::TOptionType<NOptions::TBackoff, TNoneBackoff, TOptions...>;
        using TReclaimer = NHp::NOptions::TOptionType<NOptions::TReclaimer, THazardPointerReclaimer, TOptions...>;

        using TType = THarrisMichaelHashTable<
            std::pair<const TKey, TValue>,
            THarrisMichaelHashTablePolicy<THasher, TKeyCompare, TMapKeySelect<TKey>, TReclaimer, TBackoff, NumOfBuckets>>;
    };

} // namespace NLockFreeMap::NPrivate
