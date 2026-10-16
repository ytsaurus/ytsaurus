#ifndef INCLUDE_HARRIS_MICHAEL_LIST_INL_H
    #error "include harris_michael_list.h instead"
    #include "harris_michael_list.h"
#endif

#include "accessor.h"
#include "backoff.h"
#include "key_select.h"
#include "options.h"
#include "reclaimer.h"

namespace NLockFreeMap::NPrivate {

    // ============================================================================================
    // THarrisMichaelList
    // ============================================================================================

    template <class TValue, class TPolicy>
    struct THarrisMichaelList<TValue, TPolicy>::TNode: public TReclaimer::template TEnableRetirement<TNode> {
        template <class... TArgs>
        explicit TNode(TArgs&&... args) noexcept(std::is_nothrow_constructible_v<TValue, TArgs&&...>)
            : Value(std::forward<TArgs>(args)...)
        {}

        TValue Value;
        std::atomic<TNodeMarkedPtr> Next{};
    };

    template <class TValue, class TPolicy>
    template <bool IsConst>
    class THarrisMichaelList<TValue, TPolicy>::TIterator {
        friend class THarrisMichaelList<TValue, TPolicy>;
        class TCtorImplTag {};

        class TDummyNonConstIter;
        using TNonConstIter = std::conditional_t<IsConst, TIterator<false>, TDummyNonConstIter>;

    public:
        using value_type = TValue;
        using difference_type = std::ptrdiff_t;
        using pointer = std::conditional_t<IsConst, const TValue*, TValue*>;
        using reference = std::conditional_t<IsConst, const TValue&, TValue&>;
        using iterator_category = std::forward_iterator_tag;

        TIterator() = default;

        TIterator(const TNonConstIter& other)
            : TIterator(TCtorImplTag{}, other)
        {}

        TIterator(const TIterator& other)
            : TIterator(TCtorImplTag{}, other)
        {}

        TIterator(TNonConstIter&& other)
            : TIterator(TCtorImplTag{}, other)
        {}

        TIterator(TIterator&& other) noexcept
            : TIterator(TCtorImplTag{}, std::move(other))
        {}

        template <bool _IsConst>
        TIterator(TCtorImplTag, const TIterator<_IsConst>& other)
            : Current_(other.Current_)
            , List_(other.List_)
        {
            if (other.Guard_) {
                Guard_ = List_->Reclaimer_.MakeGuard();
                Guard_.CopyProtection(other.Guard_);
            }
        }

        template <bool _IsConst>
        TIterator(TCtorImplTag, TIterator<_IsConst>&& other) noexcept
            : Current_(other.Current_)
            , Guard_(std::move(other.Guard_))
            , List_(other.List_)
        {}

        template <bool _IsConst>
        requires(IsConst || _IsConst == IsConst)
        TIterator& operator=(const TIterator<_IsConst>& other) {
            TIterator{other}.Swap(*this);
            return *this;
        }

        template <bool _IsConst>
        requires(IsConst || _IsConst == IsConst)
        TIterator& operator=(TIterator<_IsConst>&& other) {
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
            return Current_->Value;
        }

        pointer operator->() const noexcept {
            return std::addressof(this->operator*());
        }

        void Swap(TIterator& other) noexcept {
            std::ranges::swap(Current_, other.Current_);
            std::ranges::swap(Guard_, other.Guard_);
            std::ranges::swap(List_, other.List_);
        }

        friend void swap(TIterator& left, TIterator& right) noexcept { // NOLINT(readability-identifier-naming)
            left.Swap(right);
        }

        friend bool operator==(const TIterator& left, const TIterator& right) noexcept {
            return left.Current_ == right.Current_;
        }

    private:
        TIterator(TNodePtr current, TGuard guard, const THarrisMichaelList* list) noexcept
            : Current_(current)
            , Guard_(std::move(guard))
            , List_(list)
        {}

        void Increment() {
            if (!Current_) {
                return;
            }

            auto pos = List_->MakePosition();
            auto backoff = TBackoff{};

            pos.Next = pos.NextGuard.Protect(Current_->Next, UnpackMarkedPtr);

            if (pos.Next.IsMarked()) {
                List_->Find(List_->KeySelect_(Current_->Value), pos, backoff);
                Current_ = pos.Cur;
                Guard_ = std::move(pos.CurGuard);
            } else {
                Current_ = pos.Next;
                Guard_ = std::move(pos.NextGuard);
            }
        }

    private:
        TNodePtr Current_{};
        TGuard Guard_{};
        const THarrisMichaelList* List_{};
    };

    template <class TValue, class TPolicy>
    template <bool IsConst>
    struct THarrisMichaelList<TValue, TPolicy>::TAccessor: public NPrivate::TAccessor<TValue, TGuard> {
        using TBase = NPrivate::TAccessor<TValue, TGuard>;
        using TBase::TBase;
    };

    template <class TValue, class TPolicy>
    THarrisMichaelList<TValue, TPolicy>::THarrisMichaelList(const TKeyCompare& compare, const TReclaimer& reclaimer) noexcept
        : KeyCompare_(compare)
        , Reclaimer_(reclaimer)
    {}

    template <class TValue, class TPolicy>
    THarrisMichaelList<TValue, TPolicy>::~THarrisMichaelList() {
        auto current = Head_.load(std::memory_order_acquire);
        while (current) {
            auto next = current->Next.load(std::memory_order_acquire);
            delete current.Get();
            current = next;
        }
    }

    template <class TValue, class TPolicy>
    bool THarrisMichaelList<TValue, TPolicy>::insert(const TValue& value) {
        return emplace(value);
    }

    template <class TValue, class TPolicy>
    bool THarrisMichaelList<TValue, TPolicy>::insert(TValue&& value) {
        return emplace(std::move(value));
    }

    template <class TValue, class TPolicy>
    template <class _TValue>
    requires std::constructible_from<TValue, _TValue&&>
    bool THarrisMichaelList<TValue, TPolicy>::insert(_TValue&& value) {
        return emplace(std::forward<_TValue>(value));
    }

    template <class TValue, class TPolicy>
    template <class... TArgs>
    requires std::constructible_from<TValue, TArgs&&...>
    bool THarrisMichaelList<TValue, TPolicy>::emplace(TArgs&&... args) {
        if constexpr (std::is_invocable_v<TKeySelect, TArgs...>) {
            auto&& key = KeySelect_(args...);
            auto nodeFactory = [args = std::forward_as_tuple(args...)]() {
                return std::apply(
                    [](auto&&... args) {
                        return MakeHolder<TNode>(std::forward<decltype(args)>(args)...);
                    },
                    std::move(args)
                );
            };
            return InsertNode(key, nodeFactory);
        } else {
            auto newNode = MakeHolder<TNode>(std::forward<TArgs>(args)...);
            return InsertNode(std::move(newNode));
        }
    }

    template <class TValue, class TPolicy>
    bool THarrisMichaelList<TValue, TPolicy>::erase(const TKey& key) {
        auto [node, guard] = ExtractNode(key);
        return node;
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelList<TValue, TPolicy>::extract(const TKey& key) -> accessor {
        auto [node, guard] = ExtractNode(key);
        if (node) {
            return {std::addressof(node->Value), std::move(guard)};
        }
        return {};
    }

    template <class TValue, class TPolicy>
    void THarrisMichaelList<TValue, TPolicy>::clear() {
        auto headGuard = Reclaimer_.MakeGuard();
        auto backoff = TBackoff{};
        auto pos = MakePosition();
        while (true) {
            auto head = headGuard.Protect(Head_, UnpackMarkedPtr);
            if (!head) {
                break;
            }
            if (Find(KeySelect_(head->Value), pos, backoff) && pos.Cur == head.Get()) {
                Unlink(pos);
            }
        }
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelList<TValue, TPolicy>::at(const TKey& key) -> accessor {
        auto pos = MakePosition();
        auto backoff = TBackoff{};
        if (Find(key, pos, backoff)) {
            return accessor{std::addressof(pos.Cur->Value), std::move(pos.CurGuard)};
        }
        return accessor{};
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelList<TValue, TPolicy>::at(const TKey& key) const -> const_accessor {
        auto pos = MakePosition();
        auto backoff = TBackoff{};
        if (Find(key, pos, backoff)) {
            return const_accessor{std::addressof(pos.Cur->Value), std::move(pos.CurGuard)};
        }
        return accessor{};
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelList<TValue, TPolicy>::find(const TKey& key) -> iterator {
        auto pos = MakePosition();
        auto backoff = TBackoff{};
        if (Find(key, pos, backoff)) {
            return iterator{pos.Cur, std::move(pos.CurGuard), this};
        }
        return iterator{};
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelList<TValue, TPolicy>::find(const TKey& key) const -> const_iterator {
        auto pos = MakePosition();
        auto backoff = TBackoff{};
        if (Find(key, pos, backoff)) {
            return const_iterator{pos.Cur, std::move(pos.CurGuard), this};
        }
        return const_iterator{};
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelList<TValue, TPolicy>::lower_bound(const TKey& key) -> iterator {
        auto pos = MakePosition();
        auto backoff = TBackoff{};
        Find(key, pos, backoff);
        if (pos.Cur) {
            return iterator{pos.Cur, std::move(pos.CurGuard), this};
        }
        return iterator{};
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelList<TValue, TPolicy>::lower_bound(const TKey& key) const -> const_iterator {
        auto pos = MakePosition();
        auto backoff = TBackoff{};
        Find(key, pos, backoff);
        if (pos.Cur) {
            return const_iterator{pos.Cur, std::move(pos.CurGuard), this};
        }
        return const_iterator{};
    }

    template <class TValue, class TPolicy>
    bool THarrisMichaelList<TValue, TPolicy>::contains(const TKey& key) const {
        auto pos = MakePosition();
        auto backoff = TBackoff{};
        return Find(key, pos, backoff);
    }

    template <class TValue, class TPolicy>
    bool THarrisMichaelList<TValue, TPolicy>::empty() const noexcept {
        return !Head_.load(std::memory_order_relaxed);
    }

    template <class TValue, class TPolicy>
    std::size_t THarrisMichaelList<TValue, TPolicy>::size() const noexcept {
        return Size_.Size();
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelList<TValue, TPolicy>::begin() noexcept -> iterator {
        auto [node, guard] = GetHead();
        return iterator{node, std::move(guard), this};
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelList<TValue, TPolicy>::end() noexcept -> iterator {
        return iterator{};
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelList<TValue, TPolicy>::cbegin() const noexcept -> const_iterator {
        auto [node, guard] = GetHead();
        return const_iterator{node, std::move(guard), this};
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelList<TValue, TPolicy>::cend() const noexcept -> const_iterator {
        return const_iterator{};
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelList<TValue, TPolicy>::begin() const noexcept -> const_iterator {
        auto [node, guard] = GetHead();
        return const_iterator{node, std::move(guard), this};
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelList<TValue, TPolicy>::end() const noexcept -> const_iterator {
        return const_iterator{};
    }

    template <class TValue, class TPolicy>
    struct THarrisMichaelList<TValue, TPolicy>::TPosition {
        std::atomic<TNodeMarkedPtr>* PrevPointer{};
        TGuard PrevGuard{};

        TNodePtr Cur{};
        TGuard CurGuard{};

        TNodeMarkedPtr Next{};
        TGuard NextGuard{};
    };

    template <class TValue, class TPolicy>
    auto THarrisMichaelList<TValue, TPolicy>::MakePosition() const noexcept -> TPosition {
        return TPosition{
            .PrevGuard = Reclaimer_.MakeGuard(),
            .CurGuard = Reclaimer_.MakeGuard(),
            .NextGuard = Reclaimer_.MakeGuard(),
        };
    }

    template <class TValue, class TPolicy>
    bool THarrisMichaelList<TValue, TPolicy>::Unlink(const TPosition& pos) {
        auto expectedNext = TNodeMarkedPtr{pos.Next, 0};
        if (
            pos.Cur->Next.compare_exchange_weak(
                expectedNext, TNodeMarkedPtr{pos.Next, 1},
                std::memory_order_relaxed,
                std::memory_order_relaxed
            )
        ) {
            auto expectedCur = TNodeMarkedPtr{pos.Cur, 0};
            if (
                pos.PrevPointer->compare_exchange_weak(
                    expectedCur, expectedNext,
                    std::memory_order_release,
                    std::memory_order_relaxed
                )
            ) {
                Reclaimer_.Retire(expectedCur.Get());
            }
            Size_.Dec();
            return true;
        }
        return false;
    }

    template <class TValue, class TPolicy>
    bool THarrisMichaelList<TValue, TPolicy>::Link(const TPosition& pos, TNodePtr newNode) {
        auto expected = TNodeMarkedPtr{pos.Cur, 0};
        newNode->Next.store(expected, std::memory_order_release);
        if (
            pos.PrevPointer->compare_exchange_weak(
                expected, TNodeMarkedPtr{newNode, 0},
                std::memory_order_release,
                std::memory_order_relaxed
            )
        ) {
            Size_.Inc();
            return true;
        } else {
            newNode->Next.store(nullptr, std::memory_order_relaxed);
            return false;
        }
    }

    template <class TValue, class TPolicy>
    bool THarrisMichaelList<TValue, TPolicy>::Find(const TKey& key, TPosition& pos, TBackoff& backoff) const {
        auto head = const_cast<std::atomic<TNodeMarkedPtr>*>(&Head_);

    try_again:
        pos.PrevPointer = head;
        pos.Cur = pos.CurGuard.Protect(*pos.PrevPointer, UnpackMarkedPtr);

        while (true) {
            if (!pos.Cur) {
                pos.Cur = nullptr;
                pos.Next = nullptr;
                return false;
            }

            pos.Next = pos.NextGuard.Protect(pos.Cur->Next, UnpackMarkedPtr);

            if (pos.PrevPointer->load(std::memory_order_relaxed).Raw() != pos.Cur) {
                backoff();
                goto try_again;
            }

            if (pos.Next.IsMarked()) {
                auto expected = TNodeMarkedPtr{pos.Cur, 0};
                if (
                    !pos.PrevPointer->compare_exchange_weak(
                        expected, TNodeMarkedPtr{pos.Next, 0},
                        std::memory_order_release,
                        std::memory_order_relaxed
                    )
                ) {
                    backoff();
                    goto try_again;
                }
                Reclaimer_.Retire(pos.Cur);
            } else {
                if (!KeyCompare_(KeySelect_(pos.Cur->Value), key)) {
                    return !KeyCompare_(key, KeySelect_(pos.Cur->Value));
                }
                pos.PrevPointer = &(pos.Cur->Next);
                pos.PrevGuard.swap(pos.CurGuard);
            }
            pos.Cur = pos.Next;
            pos.CurGuard.swap(pos.NextGuard);
        }
    }

    template <class TValue, class TPolicy>
    bool THarrisMichaelList<TValue, TPolicy>::InsertNode(THolder<TNode> newNode) {
        auto backoff = TBackoff{};
        auto pos = MakePosition();
        const auto& key = KeySelect_(newNode->Value);

        while (true) {
            if (Find(key, pos, backoff)) {
                return false;
            }
            if (Link(pos, newNode.Get())) {
                (void)newNode.Release();
                return true;
            }
            backoff();
        }
    }

    template <class TValue, class TPolicy>
    template <class TNodeFactory>
    bool THarrisMichaelList<TValue, TPolicy>::InsertNode(const TKey& key, TNodeFactory&& factory) {
        auto backoff = TBackoff{};
        auto pos = MakePosition();
        auto newNode = THolder<TNode>{};

        while (true) {
            if (Find(key, pos, backoff)) {
                return false;
            }
            if (!newNode) {
                newNode = factory();
            }
            if (Link(pos, newNode.Get())) {
                (void)newNode.Release();
                return true;
            }
            backoff();
        }
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelList<TValue, TPolicy>::ExtractNode(const TKey& key) -> std::pair<TNodePtr, TGuard> {
        auto backoff = TBackoff{};
        auto pos = MakePosition();

        while (Find(key, pos, backoff)) {
            if (Unlink(pos)) {
                return {pos.Cur, std::move(pos.CurGuard)};
            }
            backoff();
        }
        return {};
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelList<TValue, TPolicy>::GetHead() const -> std::pair<TNodePtr, TGuard> {
        auto headGuard = Reclaimer_.MakeGuard();
        auto head = headGuard.Protect(Head_, UnpackMarkedPtr);
        if (head) {
            return {head, std::move(headGuard)};
        }
        return {};
    }

    template <class TValue, class TPolicy>
    auto THarrisMichaelList<TValue, TPolicy>::UnpackMarkedPtr(const TNodeMarkedPtr markedPtr) noexcept -> TNodePtr {
        return markedPtr.Get();
    }

    template <class TValue, class... TOptions>
    struct TMakeHarrisMichaelListSet {
        using TKeyCompare = NHp::NOptions::TOptionType<NOptions::TCompare, TLess<TValue>, TOptions...>;
        using TBackoff = NHp::NOptions::TOptionType<NOptions::TBackoff, TNoneBackoff, TOptions...>;
        using TReclaimer = NHp::NOptions::TOptionType<NOptions::TReclaimer, THazardPointerReclaimer, TOptions...>;

        using TType = THarrisMichaelList<TValue, THarrisMichaelListPolicy<TKeyCompare, TSetKeySelect<TValue>, TReclaimer, TBackoff>>;
    };

    template <class TKey, class TValue, class... TOptions>
    struct TMakeHarrisMichaelListMap {
        using TKeyCompare = typename NHp::NOptions::TOptionType<NOptions::TCompare, TLess<TKey>, TOptions...>;
        using TBackoff = typename NHp::NOptions::TOptionType<NOptions::TBackoff, TNoneBackoff, TOptions...>;
        using TReclaimer = typename NHp::NOptions::TOptionType<NOptions::TReclaimer, THazardPointerReclaimer, TOptions...>;

        using TType = THarrisMichaelList<std::pair<const TKey, TValue>, THarrisMichaelListPolicy<TKeyCompare, TMapKeySelect<TKey>, TReclaimer, TBackoff>>;
    };

} // namespace NLockFreeMap::NPrivate
