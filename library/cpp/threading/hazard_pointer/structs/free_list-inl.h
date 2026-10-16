#ifndef INCLUDE_FREE_LIST_INL_H
    #error "include free_list.h instead"
    #include "free_list.h"
#endif

#include <concepts>

namespace NHp::NStructs {

    template <class TValue>
    struct TFreeListBaseNodeTraits {
        static TFreeListNode<TValue>* AsNode(TValue* value) {
            static_assert(requires { std::derived_from<TValue, TFreeListNode<TValue>>; });
            return static_cast<TFreeListNode<TValue>*>(value);
        }
    };

    // ============================================================================================
    // TFreeListNode
    // ============================================================================================

    template <class TValue>
    TFreeListNode<TValue>::TFreeListNode(const TFreeListNode&) noexcept
        : TFreeListNode()
    {}

    template <class TValue>
    TFreeListNode<TValue>& TFreeListNode<TValue>::operator=(const TFreeListNode&) {
        return *this;
    }

    template <class TValue>
    bool TFreeListNode<TValue>::IsLinked() const noexcept {
        return Next_;
    }

    // ============================================================================================
    // TFreeList
    // ============================================================================================

    template <class TValue, class TNodeTraits>
    void TFreeList<TValue, TNodeTraits>::PushToLocal(TReference value) noexcept {
        auto node = TNodeTraits::AsNode(&value);
        node->Next_ = LocalHead_;
        LocalHead_ = &value;
    }

    template <class TValue, class TNodeTraits>
    void TFreeList<TValue, TNodeTraits>::PushToGlobal(TReference value) noexcept {
        auto newNode = TNodeTraits::AsNode(&value);
        auto head = GlobalHead_.load(std::memory_order_relaxed);
        do {
            newNode->Next_ = head;
        } while (!GlobalHead_.compare_exchange_weak(
            head, &value,
            std::memory_order_release,
            std::memory_order_relaxed
        ));
    }

    template <class TValue, class TNodeTraits>
    TFreeList<TValue, TNodeTraits>::TPointer TFreeList<TValue, TNodeTraits>::Pop() noexcept {
        if (!LocalHead_) {
            LocalHead_ = GlobalHead_.exchange(nullptr, std::memory_order_acquire);
        }
        if (LocalHead_) {
            auto result = LocalHead_;
            auto resultNode = TNodeTraits::AsNode(result);
            LocalHead_ = resultNode->Next_;
            return result;
        }
        return nullptr;
    }

    template <class TValue, class TNodeTraits>
    bool TFreeList<TValue, TNodeTraits>::Empty() const noexcept {
        if (LocalHead_) {
            return false;
        } else {
            return !GlobalHead_.load(std::memory_order_relaxed);
        }
    }

} // namespace NHp::NStructs
