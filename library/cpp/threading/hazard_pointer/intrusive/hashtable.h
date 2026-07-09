#pragma once

#include "generic_hook.h"
#include "size_tracker.h"

#include <util/system/compiler.h>
#include <util/system/yassert.h>

#include <cstddef>
#include <iterator>
#include <memory>
#include <type_traits>
#include <utility>

namespace NHp::NIntrusive::NPrivate {

    template <class TNodeTraits, bool IsFakeNode = true>
    class TBucketValue {
    public:
        using TNode = typename TNodeTraits::TNode;
        using TNodePtr = typename TNodeTraits::TNodePtr;
        using TConstNodePtr = typename TNodeTraits::TConstNodePtr;

    public:
        TBucketValue() = default;

        TNodePtr AsNodePtr() noexcept {
            return std::pointer_traits<TNodePtr>::pointer_to(*reinterpret_cast<TNode*>(this));
        }

        TConstNodePtr AsNodePtr() const noexcept {
            return std::pointer_traits<TConstNodePtr>::pointer_to(*reinterpret_cast<const TNode*>(this));
        }

        TNodePtr GetBucketBegin() const noexcept {
            TConstNodePtr thisNodePtr = AsNodePtr();
            TNodePtr next = TNodeTraits::GetNext(thisNodePtr);
            if (next) {
                return TNodeTraits::GetNext(next);
            }
            return TNodePtr{};
        }

    private:
        TNodePtr Next_{};
    };

    template <class TNodeTraits>
    class TBucketValue<TNodeTraits, false>: private TNodeTraits::TNode {
    public:
        using TNode = typename TNodeTraits::TNode;
        using TNodePtr = typename TNodeTraits::TNodePtr;
        using TConstNodePtr = typename TNodeTraits::TConstNodePtr;

    public:
        TBucketValue() = default;

        TNodePtr AsNodePtr() noexcept {
            return std::pointer_traits<TNodePtr>::pointer_to(*static_cast<TNode*>(this));
        }

        TConstNodePtr AsNodePtr() const noexcept {
            return std::pointer_traits<TConstNodePtr>::pointer_to(*static_cast<const TNode*>(this));
        }

        TNodePtr GetBucketBegin() const noexcept {
            TConstNodePtr thisNodePtr = AsNodePtr();
            TNodePtr next = TNodeTraits::GetNext(thisNodePtr);
            if (next) {
                return TNodeTraits::GetNext(next);
            }
            return TNodePtr{};
        }
    };

    template <class _TBucketPtr, class _TSizeType>
    class TBucketTraitsImpl {
    public:
        using TBucketPtr = _TBucketPtr;
        using TSizeType = _TSizeType;

    public:
        TBucketTraitsImpl(TBucketPtr buckets, TSizeType size) noexcept
            : Buckets_(buckets)
            , Size_(size)
        {
        }

        TBucketTraitsImpl(const TBucketTraitsImpl& other) noexcept = default;

        TBucketTraitsImpl& operator=(const TBucketTraitsImpl&) noexcept = default;

        void swap(TBucketTraitsImpl& other) noexcept {
            using std::swap;
            swap(Buckets_, other.Buckets_);
            swap(Size_, other.Size_);
        }

        friend void swap(TBucketTraitsImpl& left, TBucketTraitsImpl& right) noexcept { // NOLINT(readability-identifier-naming)
            left.swap(right);
        }

        TBucketPtr data() const noexcept {
            return Buckets_;
        }

        TSizeType size() const noexcept {
            return Size_;
        }

    private:
        TBucketPtr Buckets_;
        TSizeType Size_;
    };

    template <class TNodeTraits>
    struct THashTableAlgo {
        using TNodePtr = typename TNodeTraits::TNodePtr;
        using TConstNodePtr = typename TNodeTraits::TConstNodePtr;

        static void Init(TNodePtr thisNode) noexcept {
            TNodeTraits::SetNext(thisNode, TNodePtr{});
            TNodeTraits::SetPrev(thisNode, TNodePtr{});
        }

        static bool IsLinked(TConstNodePtr thisNode) noexcept {
            return TNodeTraits::GetPrev(thisNode) || TNodeTraits::GetNext(thisNode);
        }

        static std::size_t Distance(TConstNodePtr first, TConstNodePtr last) noexcept {
            std::size_t result = 0;
            while (first != last) {
                ++result;
                first = TNodeTraits::GetNext(first);
            }
            return result;
        }

        static bool LastInBucket(TConstNodePtr thisNode) noexcept {
            TNodePtr next = TNodeTraits::GetNext(thisNode);
            return !next || thisNode != TNodeTraits::GetPrev(next);
        }

        static bool FirstInBucket(TConstNodePtr thisNode) noexcept {
            TNodePtr prev = TNodeTraits::GetPrev(thisNode);
            return !prev || thisNode != TNodeTraits::GetNext(prev);
        }

        static void Unlink(TNodePtr thisNode) noexcept {
            if (!IsLinked(thisNode)) {
                return;
            }

            TNodePtr prev = TNodeTraits::GetPrev(thisNode);
            TNodePtr next = TNodeTraits::GetNext(thisNode);

            if (LastInBucket(thisNode) && FirstInBucket(thisNode)) {
                TNodePtr bucketPtr = prev;

                if (bucketPtr) {
                    prev = TNodeTraits::GetNext(bucketPtr);
                    TNodeTraits::SetNext(prev, next);
                }
                if (next) {
                    TNodePtr nextBucketPtr = TNodeTraits::GetPrev(next);
                    TNodeTraits::SetNext(nextBucketPtr, prev);
                }

                TNodeTraits::SetNext(bucketPtr, TNodePtr{});
            } else if (FirstInBucket(thisNode)) {
                TNodePtr bucketPtr = prev;
                TNodeTraits::SetPrev(next, bucketPtr);

                if (bucketPtr) {
                    prev = TNodeTraits::GetNext(bucketPtr);
                    TNodeTraits::SetNext(prev, next);
                }
            } else if (LastInBucket(thisNode)) {
                TNodeTraits::SetNext(prev, next);

                if (next) {
                    TNodePtr nextBucketPtr = TNodeTraits::GetPrev(next);
                    TNodeTraits::SetNext(nextBucketPtr, prev);
                }
            } else {
                TNodeTraits::SetNext(prev, next);
                TNodeTraits::SetPrev(next, prev);
            }

            Init(thisNode);
        }

        static void Link(TNodePtr head, TNodePtr bucket, TNodePtr newNode) noexcept {
            TNodePtr prevFirstNode = TNodeTraits::GetNext(bucket);
            if (!prevFirstNode) {
                if (IsLinked(head)) {
                    TNodePtr prevHeadNext = TNodeTraits::GetNext(head);
                    TNodePtr prevHeadBucket = TNodeTraits::GetPrev(prevHeadNext);

                    TNodeTraits::SetNext(newNode, prevHeadNext);
                    TNodeTraits::SetNext(prevHeadBucket, newNode);
                }
                TNodeTraits::SetNext(head, newNode);

                TNodeTraits::SetNext(bucket, head);
                TNodeTraits::SetPrev(newNode, bucket);
            } else {
                TNodePtr firstBucketNode = TNodeTraits::GetNext(prevFirstNode);
                LinkAfter(firstBucketNode, newNode);
            }
        }

        static void LinkAfter(TNodePtr prevNode, TNodePtr newNode) noexcept {
            TNodePtr nextNode = TNodeTraits::GetNext(prevNode);
            bool lastInBucket = LastInBucket(prevNode);

            TNodeTraits::SetNext(prevNode, newNode);

            TNodeTraits::SetNext(newNode, nextNode);
            TNodeTraits::SetPrev(newNode, prevNode);

            if (lastInBucket) {
                if (nextNode) {
                    TNodePtr bucketPtr = TNodeTraits::GetPrev(nextNode);
                    TNodeTraits::SetNext(bucketPtr, newNode);
                }
            } else {
                TNodeTraits::SetPrev(nextNode, newNode);
            }
        }

        static void SwapHeads(TNodePtr thisNode, TNodePtr otherNode) noexcept {
            if (thisNode != otherNode) {
                TNodePtr thisNext = TNodeTraits::GetNext(thisNode);
                TNodePtr otherNext = TNodeTraits::GetNext(otherNode);

                TNodePtr thisNextPrev{};
                TNodePtr otherNextPrev{};

                if (thisNext) {
                    thisNextPrev = TNodeTraits::GetPrev(thisNext);
                }
                if (otherNext) {
                    otherNextPrev = TNodeTraits::GetPrev(otherNext);
                }

                TNodeTraits::SetNext(thisNode, otherNext);
                TNodeTraits::SetNext(otherNode, thisNext);

                if (thisNextPrev) {
                    TNodeTraits::SetNext(thisNextPrev, otherNode);
                }
                if (otherNextPrev) {
                    TNodeTraits::SetNext(otherNextPrev, thisNode);
                }
            }
        }
    };

    template <class TVoidPtr, bool StoreHash>
    class THashTableNode {
        template <class, bool>
        friend class THashTableNodeTraits;

        using TPointer = typename std::pointer_traits<TVoidPtr>::template rebind<THashTableNode>;
        using TConstPointer = typename std::pointer_traits<TVoidPtr>::template rebind<const THashTableNode>;

        TPointer Next_{};
        TPointer Prev_{};

        std::size_t Hash_{};
    };

    template <class TVoidPtr>
    class THashTableNode<TVoidPtr, false> {
        template <class, bool>
        friend class THashTableNodeTraits;

        using TPointer = typename std::pointer_traits<TVoidPtr>::template rebind<THashTableNode>;
        using TConstPointer = typename std::pointer_traits<TVoidPtr>::template rebind<const THashTableNode>;

        TPointer Next_{};
        TPointer Prev_{};
    };

    template <class TVoidPtr, bool _StoreHash>
    struct THashTableNodeTraits {
        using TNode = THashTableNode<TVoidPtr, _StoreHash>;
        using TNodePtr = typename TNode::TPointer;
        using TConstNodePtr = typename TNode::TConstPointer;
        static constexpr bool StoreHash = _StoreHash;

        static void SetNext(TNodePtr thisNode, TNodePtr next) noexcept {
            thisNode->Next_ = next;
        }

        static TNodePtr GetNext(TConstNodePtr thisNode) noexcept {
            return thisNode->Next_;
        }

        static void SetPrev(TNodePtr thisNode, TNodePtr prev) noexcept {
            thisNode->Prev_ = prev;
        }

        static TNodePtr GetPrev(TConstNodePtr thisNode) noexcept {
            return thisNode->Prev_;
        }

        static std::size_t GetHash(TConstNodePtr thisNode) noexcept {
            return thisNode->Hash_;
        }

        static void SetHash(TNodePtr thisNode, std::size_t hash) noexcept {
            thisNode->Hash_ = hash;
        }
    };

    template <class TTypes, bool IsConst>
    class THashIterator {
        template <class, class, class, class, class, class, bool, bool>
        friend class TIntrusiveHashTable;
        friend class THashIterator<TTypes, true>;

        class TDummyNonConstIter;
        using TNonConstIter =
            typename std::conditional_t<IsConst, THashIterator<TTypes, false>, TDummyNonConstIter>;

        using TValueTraits = typename TTypes::TValueTraits;
        using TNodeTraits = typename TValueTraits::TNodeTraits;
        using TNodePtr = typename TNodeTraits::TNodePtr;

    public:
        using value_type = typename TTypes::value_type;
        using pointer = std::conditional_t<IsConst, typename TTypes::const_pointer, typename TTypes::pointer>;
        using reference = std::conditional_t<IsConst, typename TTypes::const_reference,
                                             typename TTypes::reference>;
        using difference_type = typename TTypes::difference_type;
        using iterator_category = std::forward_iterator_tag;

    private:
        THashIterator(TNodePtr currentNode, const TValueTraits& valueTraits) noexcept
            : CurrentNode_(currentNode)
            , ValueTraits_(valueTraits)
        {
        }

    public:
        THashIterator() noexcept = default;

        THashIterator(const TNonConstIter& other) noexcept
            : CurrentNode_(other.CurrentNode_)
            , ValueTraits_(other.ValueTraits_)
        {
        }

        THashIterator& operator=(const TNonConstIter& other) noexcept {
            CurrentNode_ = other.CurrentNode_;
            ValueTraits_ = other.ValueTraits_;
            return *this;
        }

        THashIterator& operator++() noexcept {
            Increment();
            return *this;
        }

        THashIterator operator++(int) noexcept {
            THashIterator result(*this);
            Increment();
            return result;
        }

        reference operator*() const noexcept {
            return *operator->();
        }

        pointer operator->() const noexcept {
            return ValueTraits_.ToValuePtr(CurrentNode_);
        }

        friend bool operator==(const THashIterator& left, const THashIterator& right) noexcept {
            return left.CurrentNode_ == right.CurrentNode_;
        }

        friend bool operator!=(const THashIterator& left, const THashIterator& right) noexcept {
            return !(left == right);
        }

    private:
        void Increment() noexcept {
            CurrentNode_ = TNodeTraits::GetNext(CurrentNode_);
        }

    private:
        TNodePtr CurrentNode_{};
        Y_NO_UNIQUE_ADDRESS TValueTraits ValueTraits_{};
    };

    template <class TTypes, class TNodeAlgo, bool IsConst>
    class THashLocalIterator {
        template <class, class, class, class, class, class, bool, bool>
        friend class TIntrusiveHashTable;
        friend class THashLocalIterator<TTypes, TNodeAlgo, true>;

        class TDummyNonConstIter;
        using TNonConstIter = typename std::conditional_t<IsConst, THashLocalIterator<TTypes, TNodeAlgo, false>,
                                                          TDummyNonConstIter>;

        using TValueTraits = typename TTypes::TValueTraits;
        using TNodeTraits = typename TValueTraits::TNodeTraits;
        using TNodePtr = typename TNodeTraits::TNodePtr;

    public:
        using value_type = typename TTypes::value_type;
        using pointer = std::conditional_t<IsConst, typename TTypes::const_pointer, typename TTypes::pointer>;
        using reference = std::conditional_t<IsConst, typename TTypes::const_reference,
                                             typename TTypes::reference>;
        using difference_type = typename TTypes::difference_type;
        using iterator_category = std::forward_iterator_tag;

    private:
        THashLocalIterator(TNodePtr currentNode, const TValueTraits& valueTraits) noexcept
            : CurrentNode_(currentNode)
            , ValueTraits_(valueTraits)
        {
        }

    public:
        THashLocalIterator() noexcept = default;

        THashLocalIterator(const TNonConstIter& other) noexcept
            : CurrentNode_(other.CurrentNode_)
            , ValueTraits_(other.ValueTraits_)
        {
        }

        THashLocalIterator& operator=(const TNonConstIter& other) noexcept {
            CurrentNode_ = other.CurrentNode_;
            ValueTraits_ = other.ValueTraits_;
            return *this;
        }

        THashLocalIterator& operator++() noexcept {
            Increment();
            return *this;
        }

        THashLocalIterator operator++(int) noexcept {
            THashLocalIterator result(*this);
            Increment();
            return result;
        }

        reference operator*() const noexcept {
            return *operator->();
        }

        pointer operator->() const noexcept {
            return ValueTraits_.ToValuePtr(CurrentNode_);
        }

        friend bool operator==(const THashLocalIterator& left, const THashLocalIterator& right) noexcept {
            return left.CurrentNode_ == right.CurrentNode_;
        }

        friend bool operator!=(const THashLocalIterator& left, const THashLocalIterator& right) noexcept {
            return !(left == right);
        }

    private:
        void Increment() noexcept {
            if (TNodeAlgo::LastInBucket(CurrentNode_)) {
                CurrentNode_ = TNodePtr{};
            } else {
                CurrentNode_ = TNodeTraits::GetNext(CurrentNode_);
            }
        }

    private:
        TNodePtr CurrentNode_{};
        Y_NO_UNIQUE_ADDRESS TValueTraits ValueTraits_{};
    };

    template <class TKeyOfValue, class TValue>
    struct TGetKey {
        using TType = std::remove_cvref_t<std::invoke_result_t<TKeyOfValue, const TValue&>>;
    };

    template <class _TValueTraits, class _TBucketTraits, class TKeyOfValue, class TKeyHash, class TKeyEqual,
              class TSizeType, bool IsPower2Buckets, bool IsMulti>
    class TIntrusiveHashTable {
    public:
        using TValueTraits = _TValueTraits;
        using TNodeTraits = typename TValueTraits::TNodeTraits;
        using TBucketTraits = _TBucketTraits;

        using TSizeTracker = TSizeTracker<TSizeType, !TValueTraits::IsAutoUnlink>;
        using TNodeAlgo = THashTableAlgo<TNodeTraits>;

        using TNode = typename TNodeTraits::TNode;
        using TNodePtr = typename TNodeTraits::TNodePtr;
        using TConstNodePtr = typename TNodeTraits::TConstNodePtr;

        using TBucketPtr = typename TBucketTraits::TBucketPtr;
        using TBucketType = typename std::pointer_traits<TBucketPtr>::element_type;

        using value_type = typename TValueTraits::TValue;
        using key_type = typename TGetKey<TKeyOfValue, value_type>::TType;

        using hasher = TKeyHash;
        using key_equal = TKeyEqual;

        using pointer = typename TValueTraits::TPointer;
        using const_pointer = typename TValueTraits::TConstPointer;
        using reference = typename TValueTraits::TReference;
        using const_reference = typename TValueTraits::TConstReference;
        using difference_type = typename TValueTraits::TDifferenceType;
        using size_type = TSizeType;

        using iterator = THashIterator<TIntrusiveHashTable, false>;
        using const_iterator = THashIterator<TIntrusiveHashTable, true>;

        using local_iterator = THashLocalIterator<TIntrusiveHashTable, TNodeAlgo, false>;
        using const_local_iterator = THashLocalIterator<TIntrusiveHashTable, TNodeAlgo, true>;

    public:
        explicit TIntrusiveHashTable(
            const TBucketTraits& buckets = {},
            const hasher& hash = {},
            const key_equal& equal = {},
            const TValueTraits& valueTraits = {}
        ) noexcept
            : BucketTraits_(buckets)
            , ValueTraits_(valueTraits)
            , Hasher_(hash)
            , KeyEqual_(equal)
        {
            Construct();
        }

        template <class Iterator>
        TIntrusiveHashTable(
            Iterator begin,
            Iterator end,
            const TBucketTraits& buckets = {},
            const hasher& hash = {},
            const key_equal& equal = {},
            const TValueTraits& valueTraits = {}
        ) noexcept
            : BucketTraits_(buckets)
            , ValueTraits_(valueTraits)
            , Hasher_(hash)
            , KeyEqual_(equal)
        {
            Construct();
            insert(begin, end);
        }

        ~TIntrusiveHashTable() {
            clear();
        }

        TIntrusiveHashTable(const TIntrusiveHashTable& other) = delete;

        TIntrusiveHashTable(TIntrusiveHashTable&& other) noexcept {
            Construct();
            swap(other);
        }

        TIntrusiveHashTable& operator=(const TIntrusiveHashTable& other) = delete;

        TIntrusiveHashTable& operator=(TIntrusiveHashTable&& other) noexcept {
            TIntrusiveHashTable temp(std::move(other));
            swap(other);
            return *this;
        }

    private:
        void Construct() noexcept {
            if constexpr (IsPower2Buckets) {
                Y_ABORT_UNLESS((BucketTraits_.size() & (BucketTraits_.size() - 1)) == 0, "The number of buckets must be a power of 2.");
            }
            TNodeAlgo::Init(GetNilPtr());
        }

        TNodePtr GetNilPtr() noexcept {
            return std::pointer_traits<TNodePtr>::pointer_to(NilNode_);
        }

        TConstNodePtr GetNilPtr() const noexcept {
            return std::pointer_traits<TConstNodePtr>::pointer_to(NilNode_);
        }

        TNodePtr GetFirst() const noexcept {
            return TNodeTraits::GetNext(GetNilPtr());
        }

        TNodePtr GetEnd() const noexcept {
            return TNodePtr{};
        }

        decltype(auto) GetKey(TConstNodePtr node) const noexcept {
            const_pointer valuePtr = ValueTraits_.ToValuePtr(node);
            return KeyOfValue_(*valuePtr);
        }

        decltype(auto) GetKey(const_reference value) const noexcept {
            return KeyOfValue_(value);
        }

        std::size_t GetHash(TNodePtr node) const noexcept {
            if constexpr (TNodeTraits::StoreHash) {
                return TNodeTraits::GetHash(node);
            } else {
                return Hasher_(GetKey(node));
            }
        }

        void SetHash(TNodePtr node, std::size_t hash) const noexcept {
            if constexpr (TNodeTraits::StoreHash) {
                TNodeTraits::SetHash(node, hash);
            }
        }

        size_type GetSize() const noexcept {
            if constexpr (TSizeTracker::IsTrackingSize) {
                return SizeTracker_.GetSize();
            } else {
                return TNodeAlgo::Distance(GetFirst(), GetEnd());
            }
        }

        TBucketPtr GetBucket(size_type bucketIndex) const noexcept {
            return BucketTraits_.data() + bucketIndex;
        }

        size_type GetBucketIdx(std::size_t hash) const noexcept {
            if constexpr (IsPower2Buckets) {
                return hash & (BucketTraits_.size() - 1);
            } else {
                return hash % BucketTraits_.size();
            }
        }

        TNodePtr GetBucketBegin(size_type bucketIndex) const noexcept {
            TBucketPtr bucket = GetBucket(bucketIndex);
            return bucket->GetBucketBegin();
        }

        TNodePtr FindImpl(const key_type& key, std::size_t hash) const noexcept {
            size_type bucketIndex = GetBucketIdx(hash);
            TNodePtr current = GetBucketBegin(bucketIndex);

            while (current) {
                TNodePtr next = TNodeTraits::GetNext(current);
                if (TNodeAlgo::LastInBucket(current)) {
                    next = TNodePtr{};
                }
                if (hash == GetHash(current) && KeyEqual_(GetKey(current), key)) {
                    return current;
                }
                current = next;
            }

            return TNodePtr{};
        }

        TNodePtr FindImpl(const key_type& key) const noexcept {
            std::size_t hash = Hasher_(key);
            return FindImpl(key, hash);
        }

        std::pair<TNodePtr, TNodePtr> EqualRangeImpl(const key_type& key) const noexcept {
            std::size_t hash = Hasher_(key);

            size_type bucketIndex = GetBucketIdx(hash);
            TNodePtr current = GetBucketBegin(bucketIndex);

            while (current) {
                TNodePtr begin = current;
                while (current && hash == GetHash(current) && KeyEqual_(GetKey(current), key)) {
                    current = TNodeTraits::GetNext(current);
                }
                if (begin == current) {
                    current = TNodeTraits::GetNext(current);
                } else {
                    return {begin, current};
                }
            }

            return {TNodePtr{}, TNodePtr{}};
        }

        void InsertByRehash(reference value) noexcept {
            TNodePtr newNode = ValueTraits_.ToNodePtr(value);
            std::size_t hash = GetHash(newNode);

            TNodePtr position = FindImpl(GetKey(value), hash);

            if (position) {
                TNodeAlgo::LinkAfter(position, newNode);
            } else {
                size_type bucketIndex = GetBucketIdx(hash);
                TBucketPtr bucket = GetBucket(bucketIndex);

                TNodeAlgo::Link(GetNilPtr(), bucket->AsNodePtr(), newNode);
            }
        }

        std::pair<iterator, bool> InsertUnique(reference value) noexcept {
            std::size_t hash = Hasher_(GetKey(value));
            TNodePtr newNode = ValueTraits_.ToNodePtr(value);
            Y_ABORT_IF(TNodeAlgo::IsLinked(newNode));
            TNodePtr position = FindImpl(GetKey(value), hash);
            SetHash(newNode, hash);

            if (position) {
                return {iterator(position, ValueTraits_), false};
            } else {
                size_type bucketIndex = GetBucketIdx(hash);
                TBucketPtr bucket = GetBucket(bucketIndex);

                SizeTracker_.Increment();

                TNodeAlgo::Link(GetNilPtr(), bucket->AsNodePtr(), newNode);
                return {iterator(newNode, ValueTraits_), true};
            }
        }

        iterator InsertEqual(reference value) noexcept {
            std::size_t hash = Hasher_(GetKey(value));
            TNodePtr newNode = ValueTraits_.ToNodePtr(value);
            Y_ABORT_IF(TNodeAlgo::IsLinked(newNode));
            TNodePtr position = FindImpl(GetKey(value), hash);
            SetHash(newNode, hash);

            SizeTracker_.Increment();

            if (position) {
                TNodeAlgo::LinkAfter(position, newNode);
                return iterator(newNode, ValueTraits_);
            } else {
                size_type bucketIndex = GetBucketIdx(hash);
                TBucketPtr bucket = GetBucket(bucketIndex);

                TNodeAlgo::Link(GetNilPtr(), bucket->AsNodePtr(), newNode);
                return iterator(newNode, ValueTraits_);
            }
        }

        void EraseNode(TNodePtr node) noexcept {
            SizeTracker_.Decrement();
            TNodeAlgo::Unlink(node);
        }

        size_type EraseImpl(const key_type& key) noexcept {
            size_type result = 0;
            std::size_t hash = Hasher_(key);

            size_type bucketIndex = GetBucketIdx(hash);
            TNodePtr current = GetBucketBegin(bucketIndex);

            while (current) {
                TNodePtr next = TNodeTraits::GetNext(current);
                if (TNodeAlgo::LastInBucket(current)) {
                    next = TNodePtr{};
                }
                if (hash == GetHash(current) && KeyEqual_(GetKey(current), key)) {
                    result++;
                    EraseNode(current);
                }
                current = next;
            }
            return result;
        }

    public:
        auto insert(reference value) noexcept {
            if constexpr (IsMulti) {
                return InsertEqual(value);
            } else {
                return InsertUnique(value);
            }
        }

        template <class Iterator>
        void insert(Iterator begin, Iterator end) noexcept {
            for (; begin != end; ++begin) {
                insert(*begin);
            }
        }

        size_type erase(const key_type& key) noexcept {
            return EraseImpl(key);
        }

        void erase(const_iterator position) noexcept {
            EraseNode(position.CurrentNode_);
        }

        void erase(const_iterator begin, const_iterator end) noexcept {
            while (begin != end) {
                auto next = std::next(begin);
                erase(begin);
                begin = next;
            }
        }

        void swap(TIntrusiveHashTable& other) noexcept {
            using std::swap;
            TNodeAlgo::SwapHeads(GetNilPtr(), other.GetNilPtr());
            swap(BucketTraits_, other.BucketTraits_);
            swap(ValueTraits_, other.ValueTraits_);
            swap(Hasher_, other.Hasher_);
            swap(KeyEqual_, other.KeyEqual_);
            swap(SizeTracker_, other.SizeTracker_);
        }

        void clear() noexcept {
            TNodePtr current = GetNilPtr();
            current = TNodeTraits::GetNext(current);
            while (current) {
                TNodePtr next = TNodeTraits::GetNext(current);
                EraseNode(current);
                current = next;
            }
        }

        template <class OtherBucketTraits, class OtherKeyOfValue, class OtherKeyHash,
                  class OtherKeyEqual, class OtherSizeType, bool OtherIsPower2Buckets,
                  bool OtherIsMulti>
        void merge(TIntrusiveHashTable<TValueTraits, OtherBucketTraits, OtherKeyOfValue, OtherKeyHash,
                                       OtherKeyEqual, OtherSizeType, OtherIsPower2Buckets, OtherIsMulti>& other) noexcept {
            for (iterator it = other.begin(); it != other.end();) {
                iterator next = std::next(it);
                other.erase(it);
                insert(*it);
                it = next;
            }
        }

        template <class OtherBucketTraits, class OtherKeyOfValue, class OtherKeyHash,
                  class OtherKeyEqual, class OtherSizeType, bool OtherIsPower2Buckets,
                  bool OtherIsMulti>
        void merge(TIntrusiveHashTable<TValueTraits, OtherBucketTraits, OtherKeyOfValue, OtherKeyHash,
                                       OtherKeyEqual, OtherSizeType, OtherIsPower2Buckets, OtherIsMulti>&& other) noexcept {
            merge(other);
        }

        template <class Iterator>
        void assign(Iterator first, Iterator last) noexcept {
            clear();
            insert(first, last);
        }

        void rehash(const TBucketTraits& newBucketTraits) noexcept {
            if constexpr (IsPower2Buckets) {
                Y_ABORT_UNLESS((newBucketTraits.size() & (newBucketTraits.size() - 1)) == 0, "The number of buckets must be a power of 2.");
            }
            BucketTraits_ = newBucketTraits;
            TNodePtr current = GetFirst();
            TNodeAlgo::Init(GetNilPtr());

            while (current) {
                TNodePtr next = TNodeTraits::GetNext(current);
                TNodeAlgo::Init(current);
                pointer valuePtr = ValueTraits_.ToValuePtr(current);
                InsertByRehash(*valuePtr);
                current = next;
            }
        }

        iterator find(const key_type& key) noexcept {
            return iterator(FindImpl(key), ValueTraits_);
        }

        const_iterator find(const key_type& key) const noexcept {
            return const_iterator(FindImpl(key), ValueTraits_);
        }

        size_type count(const key_type& key) const noexcept {
            std::pair<TNodePtr, TNodePtr> range = EqualRangeImpl(key);
            return TNodeAlgo::Distance(range.first, range.second);
        }

        bool contains(const key_type& key) const noexcept {
            return FindImpl(key);
        }

        std::pair<iterator, iterator> equal_range(const key_type& key) noexcept {
            std::pair<TNodePtr, TNodePtr> res = EqualRangeImpl(key);
            return {iterator(res.first, ValueTraits_),
                    iterator(res.second, ValueTraits_)};
        }

        std::pair<const_iterator, const_iterator> equal_range(const key_type& key) const noexcept {
            std::pair<TNodePtr, TNodePtr> res = EqualRangeImpl(key);
            return {const_iterator(res.first, ValueTraits_),
                    const_iterator(res.second, ValueTraits_)};
        }

        iterator iterator_to(reference value) noexcept {
            TNodePtr node = ValueTraits_.ToNodePtr(value);
            return iterator(node, ValueTraits_);
        }

        const_iterator iterator_to(const_reference value) const noexcept {
            TNodePtr node = ValueTraits_.ToNodePtr(value);
            return const_iterator(node, ValueTraits_);
        }

        local_iterator local_iterator_to(reference value) noexcept {
            TNodePtr node = ValueTraits_.ToNodePtr(value);

            while (!TNodeAlgo::FirstInBucket(node)) {
                node = TNodeTraits::GetPrev(node);
            }
            return local_iterator(node, ValueTraits_);
        }

        const_local_iterator local_iterator_to(const_reference value) const noexcept {
            TNodePtr node = ValueTraits_.ToNodePtr(value);

            while (!TNodeAlgo::FirstInBucket(node)) {
                node = TNodeTraits::GetPrev(node);
            }
            return const_local_iterator(node, ValueTraits_);
        }

        hasher hash_function() const noexcept {
            return Hasher_;
        }

        key_equal key_eq() const noexcept {
            return KeyEqual_;
        }

        iterator begin() noexcept {
            return iterator(GetFirst(), ValueTraits_);
        }

        iterator end() noexcept {
            return iterator(GetEnd(), ValueTraits_);
        }

        const_iterator begin() const noexcept {
            return const_iterator(GetFirst(), ValueTraits_);
        }

        const_iterator end() const noexcept {
            return const_iterator(GetEnd(), ValueTraits_);
        }

        const_iterator cbegin() const noexcept {
            return const_iterator(GetFirst(), ValueTraits_);
        }

        const_iterator cend() const noexcept {
            return const_iterator(GetEnd(), ValueTraits_);
        }

        size_type bucket_count() const noexcept {
            return BucketTraits_.size();
        }

        size_type bucket_size(size_type bucketIndex) const noexcept {
            size_type size = 0;
            for (const_local_iterator it = begin(bucketIndex); it != end(bucketIndex); ++it) {
                size += 1;
            }
            return size;
        }

        size_type bucket(const key_type& key) const noexcept {
            std::size_t hash = Hasher_(key);
            return GetBucketIdx(hash);
        }

        local_iterator begin(size_type bucketIndex) noexcept {
            return local_iterator(GetBucketBegin(bucketIndex), ValueTraits_);
        }

        local_iterator end(size_type) noexcept {
            return local_iterator(GetEnd(), ValueTraits_);
        }

        const_local_iterator begin(size_type bucketIndex) const noexcept {
            return const_local_iterator(GetBucketBegin(bucketIndex), ValueTraits_);
        }

        const_local_iterator end(size_type) const noexcept {
            return const_local_iterator(GetEnd(), ValueTraits_);
        }

        const_local_iterator cbegin(size_type bucketIndex) const noexcept {
            return const_local_iterator(GetBucketBegin(bucketIndex), ValueTraits_);
        }

        const_local_iterator cend(size_type) const noexcept {
            return const_local_iterator(GetEnd(), ValueTraits_);
        }

        size_type size() const noexcept {
            return GetSize();
        }

        bool empty() const noexcept {
            return !TNodeAlgo::IsLinked(GetNilPtr());
        }

        friend bool operator==(const TIntrusiveHashTable& left,
                               const TIntrusiveHashTable& right) noexcept {
            if (left.size() != right.size()) {
                return false;
            }
            const_iterator it = left.begin();

            while (it != left.end()) {
                std::pair<const_iterator, const_iterator> leftEqualRange = left.equal_range(left.GetKey(*it));
                std::pair<const_iterator, const_iterator> rightEqualRange = right.equal_range(right.GetKey(*it));

                if (std::distance(leftEqualRange.first, leftEqualRange.second) != std::distance(rightEqualRange.first, rightEqualRange.second)) {
                    return false;
                }
                it = leftEqualRange.second;
            }
            return true;
        }

        friend bool operator!=(const TIntrusiveHashTable& left,
                               const TIntrusiveHashTable& right) noexcept {
            return !(left == right);
        }

        friend void swap(TIntrusiveHashTable& left, TIntrusiveHashTable& right) noexcept { // NOLINT(readability-identifier-naming)
            left.swap(right);
        }

    private:
        TNode NilNode_{};
        TBucketTraits BucketTraits_;
        Y_NO_UNIQUE_ADDRESS TValueTraits ValueTraits_;
        Y_NO_UNIQUE_ADDRESS TKeyOfValue KeyOfValue_{};
        Y_NO_UNIQUE_ADDRESS TKeyHash Hasher_;
        Y_NO_UNIQUE_ADDRESS TKeyEqual KeyEqual_;
        Y_NO_UNIQUE_ADDRESS TSizeTracker SizeTracker_{};
    };

    template <class THookType>
    struct THashTableDefaultHook {
        using _THashTableDefaultHook = THookType;
    };

    template <class TVoidPtr, class TTag, bool StoreHash, bool IsAutoUnlink>
    class THashTableBaseHook
        : public TGenericHook<THashTableAlgo<THashTableNodeTraits<TVoidPtr, StoreHash>>,
                              THashTableNodeTraits<TVoidPtr, StoreHash>, TTag, IsAutoUnlink>,
          public std::conditional_t<
              std::is_same_v<TTag, TDefaultHookTag>,
              THashTableDefaultHook<
                  TGenericHook<THashTableAlgo<THashTableNodeTraits<TVoidPtr, StoreHash>>,
                               THashTableNodeTraits<TVoidPtr, StoreHash>, TTag, IsAutoUnlink>>,
              TNotDefaultHook> {};

} // namespace NHp::NIntrusive::NPrivate
