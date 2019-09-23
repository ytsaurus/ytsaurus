#pragma once

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TIntrusiveStackItem
{
    T* Next = nullptr;
};

template <class TItem>
class TIntrusiveLockFreeStack
{
private:
    struct THead
    {
        std::atomic<TItem*> Pointer = {nullptr};
        size_t PopCount = 0;

        THead() = default;

        explicit THead(TItem* pointer);

    };

    struct TAtomicHead
    {
        TItem* Pointer = nullptr;
        size_t PopCount = 0;
    };

    union
    {
        THead Head_;
        std::atomic<TAtomicHead> AtomicHead_;
    };

public:
    TIntrusiveLockFreeStack();

    TIntrusiveLockFreeStack(TIntrusiveLockFreeStack&& other);

    ~TIntrusiveLockFreeStack();

    void Put(TItem* head, TItem* tail);

    void Put(TItem* item);

    TItem* Extract();

    TItem* ExtractAll();

    bool IsEmpty() const;

    void Append(TIntrusiveLockFreeStack& other);
};

template <class T>
class TLockFreeStack
{
private:
    struct TNode
        : public TIntrusiveStackItem<TNode>
    {
        T Value;

        TNode() = default;

        explicit TNode(T&& value);
    };

    TIntrusiveLockFreeStack<TNode> Impl_;

    void EraseList(TNode* node);

public:
    TLockFreeStack() = default;

    ~TLockFreeStack();

    template <typename TCallback>
    void DequeueAll(TCallback callback);

    void Append(TLockFreeStack& other);

    void Enqueue(T&& value);

    bool Dequeue(T* value);

    bool IsEmpty() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define LOCK_FREE_STACK_INL_H_
#include "lock_free_stack-inl.h"
#undef LOCK_FREE_STACK_INL_H_
