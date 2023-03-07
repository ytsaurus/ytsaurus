#ifndef LOCK_FREE_STACK_INL_H_
#error "Direct inclusion of this file is not allowed, include lock_free_stack.h"
// For the sake of sane code completion.
#include "lock_free_stack.h"
#endif


namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TItem>
TIntrusiveLockFreeStack<TItem>::THead::THead(TItem* pointer)
    : Pointer(pointer)
{ }

template <class TItem>
TIntrusiveLockFreeStack<TItem>::TIntrusiveLockFreeStack()
    : Head_()
{ }

template <class TItem>
TIntrusiveLockFreeStack<TItem>::TIntrusiveLockFreeStack(TIntrusiveLockFreeStack<TItem>&& other)
    : Head_(other.ExtractAll())
{ }

template <class TItem>
TIntrusiveLockFreeStack<TItem>::~TIntrusiveLockFreeStack()
{
    YT_VERIFY(IsEmpty());
}

template <class TItem>
void TIntrusiveLockFreeStack<TItem>::Put(TItem* head, TItem* tail)
{
    auto* current = Head_.Pointer.load(std::memory_order_relaxed);
    auto popCount = Head_.PopCount.load(std::memory_order_relaxed);

    do {
        tail->Next = current;
    } while (!CompareAndSet(&AtomicHead_, current, popCount, head, popCount));
}

template <class TItem>
void TIntrusiveLockFreeStack<TItem>::Put(TItem* item)
{
    Put(item, item);
}

template <class TItem>
TItem* TIntrusiveLockFreeStack<TItem>::Extract()
{
    auto* current = Head_.Pointer.load(std::memory_order_relaxed);
    auto popCount = Head_.PopCount.load(std::memory_order_relaxed);

    while (current) {
        if (CompareAndSet(&AtomicHead_, current, popCount, current->Next, popCount + 1)) {
            current->Next = nullptr;
            return current;
        }
    }

    return nullptr;
}

template <class TItem>
TItem* TIntrusiveLockFreeStack<TItem>::ExtractAll()
{
    auto* current = Head_.Pointer.load(std::memory_order_relaxed);
    auto popCount = Head_.PopCount.load(std::memory_order_relaxed);

    while (current) {
        if (CompareAndSet<TItem*, size_t>(&AtomicHead_, current, popCount, nullptr, popCount + 1)) {
            return current;
        }
    }

    return nullptr;
}

template <class TItem>
bool TIntrusiveLockFreeStack<TItem>::IsEmpty() const
{
    return Head_.Pointer.load() == nullptr;
}

template <class TItem>
void TIntrusiveLockFreeStack<TItem>::Append(TIntrusiveLockFreeStack<TItem>& other)
{
    auto* head = other.ExtractAll();

    if (!head) {
        return;
    }

    auto* tail = head;
    while (tail->Next) {
        tail = tail->Next;
    }

    Put(head, tail);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TLockFreeStack<T>::TNode::TNode(T&& value)
    : Value(std::move(value))
{ }

template <class T>
void TLockFreeStack<T>::EraseList(TNode* node)
{
    while (node) {
        auto* next = node->Next;
        delete node;
        node = next;
    }
}

template <class T>
TLockFreeStack<T>::~TLockFreeStack()
{
    EraseList(Impl_.ExtractAll());
}

template <class T>
template <typename TCallback>
void TLockFreeStack<T>::DequeueAll(TCallback callback)
{
    auto* head = Impl_.ExtractAll();

    auto* ptr = head;
    while (ptr) {
        callback(ptr->Value);
        ptr = ptr->Next;
    }
    EraseList(head);
}

template <class T>
void TLockFreeStack<T>::Append(TLockFreeStack& other)
{
    Impl_.Append(other.Impl_);
}

template <class T>
void TLockFreeStack<T>::Enqueue(T&& value)
{
    auto* volatile node = new TNode(std::move(value));
    Impl_.Put(node, node);
}

template <class T>
bool TLockFreeStack<T>::Dequeue(T* value)
{
    if (auto item = Impl_.Extract()) {
        *value = std::move(item->Value);
        delete item;
        return true;
    }
    return false;
}

template <class T>
bool TLockFreeStack<T>::IsEmpty() const
{
    return Impl_.IsEmpty();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
