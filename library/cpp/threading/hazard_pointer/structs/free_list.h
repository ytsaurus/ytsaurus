#pragma once

#include <atomic>

namespace NHp::NStructs {

    template <class TValue>
    class TFreeListNode {
    public:
        TFreeListNode() = default;

        TFreeListNode(const TFreeListNode&) noexcept;
        TFreeListNode& operator=(const TFreeListNode&);

        bool IsLinked() const noexcept;

    private:
        template <class, class>
        friend class TFreeList;

        TValue* Next_{nullptr};
    };

    template <class TValue>
    struct TFreeListBaseNodeTraits;

    template <class TValue, class TNodeTraits = TFreeListBaseNodeTraits<TValue>>
    class TFreeList {
    public:
        using TReference = TValue&;
        using TPointer = TValue*;

        TFreeList() = default;

        TFreeList(const TFreeList&) = delete;
        TFreeList& operator=(const TFreeList&) = delete;

        void PushToLocal(TReference value) noexcept;
        void PushToGlobal(TReference value) noexcept;

        TPointer Pop() noexcept;
        bool Empty() const noexcept;

    private:
        std::atomic<TPointer> GlobalHead_{nullptr};
        TPointer LocalHead_{nullptr};
    };

} // namespace NHp::NStructs

#define INCLUDE_FREE_LIST_INL_H
#include "free_list-inl.h"
#undef INCLUDE_FREE_LIST_INL_H
