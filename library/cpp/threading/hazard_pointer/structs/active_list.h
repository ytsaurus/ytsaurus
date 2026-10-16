#pragma once

#include <atomic>

namespace NHp::NStructs {

    template <class TValue>
    class TActiveListNode {
    public:
        TActiveListNode() = default;

        TActiveListNode(const TActiveListNode&) noexcept;
        TActiveListNode& operator=(const TActiveListNode&);

        bool IsAcquired() const noexcept;
        void Release() noexcept;
        bool TryAcquire() noexcept;

    private:
        template <class, class>
        friend class TActiveList;

        TValue* Next_{nullptr};
        std::atomic<bool> IsActive_{false};
    };

    template <class TValue>
    struct TActiveListBaseNodeTraits;

    template <class TValue, class TNodeTraits = TActiveListBaseNodeTraits<TValue>>
    class TActiveList {
        template <bool IsConst>
        class TIterator;

    public:
        using TReference = TValue&;
        using TConstReference = const TValue&;

        using TPointer = TValue*;
        using TConstPointer = const TValue*;

        using iterator = TIterator<false>;
        using const_iterator = TIterator<true>;

        TActiveList() = default;

        TActiveList(const TActiveList&) = delete;
        TActiveList& operator=(const TActiveList&) = delete;

        void Push(TReference newItem) noexcept;
        iterator AcquireFree() noexcept;

        iterator begin() noexcept;
        iterator end() noexcept;

        const_iterator cbegin() const noexcept;
        const_iterator cend() const noexcept;

        const_iterator begin() const noexcept;
        const_iterator end() const noexcept;

    private:
        std::atomic<TPointer> Head_{nullptr};
    };

} // namespace NHp::NStructs

#define INCLUDE_ACTIVE_LIST_INL_H
#include "active_list-inl.h"
#undef INCLUDE_ACTIVE_LIST_INL_H
