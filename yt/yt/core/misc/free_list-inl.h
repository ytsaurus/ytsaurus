#ifndef FREE_LIST_INL_H_
#error "Direct inclusion of this file is not allowed, include free_list.h"
// For the sake of sane code completion.
#include "free_list.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TItem>
TFreeList<TItem>::THead::THead(TItem* pointer)
    : Pointer(pointer)
{ }

template <class TItem>
TFreeList<TItem>::TFreeList()
    : Head_()
{ }

template <class TItem>
TFreeList<TItem>::TFreeList(TFreeList<TItem>&& other)
    : Head_(other.ExtractAll())
{ }

template <class TItem>
TFreeList<TItem>::~TFreeList()
{
    YT_VERIFY(IsEmpty());
}

template <class TItem>
template <class TPredicate>
Y_NO_SANITIZE("thread")
bool TFreeList<TItem>::PutIf(TItem* head, TItem* tail, TPredicate predicate)
{
    auto* current = Head_.Pointer.load(std::memory_order::relaxed);
    auto popCount = Head_.PopCount.load(std::memory_order::relaxed);

    while (predicate(current)) {
        tail->Next.store(current, std::memory_order::release);
        if (CompareAndSet(&AtomicHead_, current, popCount, head, popCount)) {
            return true;
        }
    }

    tail->Next.store(nullptr, std::memory_order::release);

    return false;
}


template <class TItem>
Y_NO_SANITIZE("thread")
void TFreeList<TItem>::Put(TItem* head, TItem* tail)
{
    auto* current = Head_.Pointer.load(std::memory_order::relaxed);
    auto popCount = Head_.PopCount.load(std::memory_order::relaxed);

    do {
        tail->Next.store(current, std::memory_order::release);
    } while (!CompareAndSet(&AtomicHead_, current, popCount, head, popCount));
}

template <class TItem>
void TFreeList<TItem>::Put(TItem* item)
{
    Put(item, item);
}

template <class TItem>
Y_NO_SANITIZE("thread")
TItem* TFreeList<TItem>::Extract()
{
    auto* current = Head_.Pointer.load(std::memory_order::relaxed);
    auto popCount = Head_.PopCount.load(std::memory_order::relaxed);

    while (current) {
        // If current node is already extracted by other thread
        // there can be any writes at address &current->Next.
        // The only guaranteed thing is that address is valid (memory is not freed).
        auto next = current->Next.load(std::memory_order::acquire);
        if (CompareAndSet(&AtomicHead_, current, popCount, next, popCount + 1)) {
            current->Next.store(nullptr, std::memory_order::release);
            return current;
        }
    }

    return nullptr;
}

template <class TItem>
TItem* TFreeList<TItem>::ExtractAll()
{
    auto* current = Head_.Pointer.load(std::memory_order::relaxed);
    auto popCount = Head_.PopCount.load(std::memory_order::relaxed);

    while (current) {
        if (CompareAndSet<TItem*, size_t>(&AtomicHead_, current, popCount, nullptr, popCount + 1)) {
            return current;
        }
    }

    return nullptr;
}

template <class TItem>
bool TFreeList<TItem>::IsEmpty() const
{
    return Head_.Pointer.load() == nullptr;
}

template <class TItem>
void TFreeList<TItem>::Append(TFreeList<TItem>& other)
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

} // namespace NYT
