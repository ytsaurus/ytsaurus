#ifndef INCLUDE_ACTIVE_LIST_INL_H
    #error "include active_list.h instead"
    #include "active_list.h"
#endif

#include <concepts>
#include <iterator>
#include <type_traits>

namespace NHp::NStructs {

    template <class TValue>
    struct TActiveListBaseNodeTraits {
        static TActiveListNode<TValue>* AsNode(TValue* value) noexcept {
            static_assert(requires { std::derived_from<TValue, TActiveListNode<TValue>>; });
            return static_cast<TActiveListNode<TValue>*>(value);
        }
    };

    // ============================================================================================
    // TActiveListNode
    // ============================================================================================

    template <class TValue>
    TActiveListNode<TValue>::TActiveListNode(const TActiveListNode&) noexcept
        : TActiveListNode()
    {}

    template <class TValue>
    TActiveListNode<TValue>& TActiveListNode<TValue>::operator=(const TActiveListNode&) {
        return *this;
    }

    template <class TValue>
    bool TActiveListNode<TValue>::IsAcquired() const noexcept {
        return IsActive_.load(std::memory_order_acquire);
    }

    template <class TValue>
    void TActiveListNode<TValue>::Release() noexcept {
        IsActive_.store(false, std::memory_order_release);
    }

    template <class TValue>
    bool TActiveListNode<TValue>::TryAcquire() noexcept {
        if (IsActive_.load(std::memory_order_relaxed)) {
            return false;
        }
        return !IsActive_.exchange(true, std::memory_order_acquire);
    }

    // ============================================================================================
    // TActiveList::TIterator
    // ============================================================================================

    template <class TValue, class TNodeTraits>
    template <bool IsConst>
    class TActiveList<TValue, TNodeTraits>::TIterator {
        class TDummyNonConstIter;
        using TNonConstIter = typename std::conditional_t<IsConst, TIterator<false>, TDummyNonConstIter>;

    public:
        using value_type = TValue;
        using pointer = std::conditional_t<IsConst, TConstPointer, TPointer>;
        using reference = std::conditional_t<IsConst, TConstReference, TReference>;
        using difference_type = std::ptrdiff_t;
        using iterator_category = std::forward_iterator_tag;

        TIterator() = default;

        TIterator(const TIterator&) = default;
        TIterator& operator=(const TIterator&) = default;

        TIterator(const TNonConstIter& other) noexcept
            : Current_(other.Current_)
        {}

        TIterator& operator=(const TNonConstIter& other) {
            std::ranges::swap(Current_, other.Current_);
            return *this;
        }

        TIterator& operator++() noexcept {
            Increment();
            return *this;
        }

        TIterator operator++(int) noexcept {
            auto result = TIterator(*this);
            Increment();
            return result;
        }

        inline reference operator*() const noexcept {
            return *operator->();
        }

        inline pointer operator->() const noexcept {
            return Current_;
        }

        friend bool operator==(const TIterator& left, const TIterator& right) noexcept = default;

    private:
        friend class TActiveList<TValue, TNodeTraits>;

        explicit TIterator(TPointer current) noexcept
            : Current_(current)
        {}

        void Increment() noexcept {
            auto currentNode = TNodeTraits::AsNode(Current_);
            Current_ = currentNode->Next_;
        }

    private:
        TPointer Current_{nullptr};
    };

    // ============================================================================================
    // TActiveList
    // ============================================================================================

    template <class TValue, class TNodeTraits>
    void TActiveList<TValue, TNodeTraits>::Push(TReference newItem) noexcept {
        auto newNode = TNodeTraits::AsNode(&newItem);
        newNode->IsActive_.store(true, std::memory_order_relaxed);
        auto curItem = Head_.load(std::memory_order_relaxed);
        do {
            newNode->Next_ = curItem;
        } while (!Head_.compare_exchange_weak(curItem, &newItem, std::memory_order_release));
    }

    template <class TValue, class TNodeTraits>
    TActiveList<TValue, TNodeTraits>::iterator TActiveList<TValue, TNodeTraits>::AcquireFree() noexcept {
        auto curItem = Head_.load(std::memory_order_acquire);
        while (curItem) {
            auto curNode = TNodeTraits::AsNode(curItem);
            if (curNode->TryAcquire()) {
                break;
            }
            curItem = curNode->Next_;
        }
        return iterator{curItem};
    }

    template <class TValue, class TNodeTraits>
    TActiveList<TValue, TNodeTraits>::iterator TActiveList<TValue, TNodeTraits>::begin() noexcept {
        return iterator{Head_.load(std::memory_order_acquire)};
    }

    template <class TValue, class TNodeTraits>
    TActiveList<TValue, TNodeTraits>::iterator TActiveList<TValue, TNodeTraits>::end() noexcept {
        return iterator{};
    }

    template <class TValue, class TNodeTraits>
    TActiveList<TValue, TNodeTraits>::const_iterator TActiveList<TValue, TNodeTraits>::cbegin() const noexcept {
        return const_iterator{Head_.load(std::memory_order_acquire)};
    }

    template <class TValue, class TNodeTraits>
    TActiveList<TValue, TNodeTraits>::const_iterator TActiveList<TValue, TNodeTraits>::cend() const noexcept {
        return const_iterator{};
    }

    template <class TValue, class TNodeTraits>
    TActiveList<TValue, TNodeTraits>::const_iterator TActiveList<TValue, TNodeTraits>::begin() const noexcept {
        return cbegin();
    }

    template <class TValue, class TNodeTraits>
    TActiveList<TValue, TNodeTraits>::const_iterator TActiveList<TValue, TNodeTraits>::end() const noexcept {
        return cend();
    }

} // namespace NHp::NStructs
