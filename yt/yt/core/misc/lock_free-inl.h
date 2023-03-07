#pragma once
#ifndef LOCK_FREE_INL_H_
#error "Direct inclusion of this file is not allowed, include lock_free.h"
// For the sake of sane code completion.
#include "lock_free.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TMultipleProducerSingleConsumerLockFreeStack<T>::TNode
{
    T Value;
    TNode* Next;

    TNode(const T& value)
        : Value(value)
        , Next(nullptr)
    { }

    TNode(T&& value)
        : Value(std::move(value))
        , Next(nullptr)
    { }
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
TMultipleProducerSingleConsumerLockFreeStack<T>::~TMultipleProducerSingleConsumerLockFreeStack()
{
    auto* current = Head_.load();
    while (current) {
        auto* next = current->Next;
        delete current;
        current = next;
    }
}

template <class T>
void TMultipleProducerSingleConsumerLockFreeStack<T>::Enqueue(const T& value)
{
    DoEnqueue(new TNode(value));
}

template <class T>
void TMultipleProducerSingleConsumerLockFreeStack<T>::Enqueue(T&& value)
{
    DoEnqueue(new TNode(std::move(value)));
}

template <class T>
void TMultipleProducerSingleConsumerLockFreeStack<T>::DoEnqueue(TNode* node)
{
    auto* expected = Head_.load(std::memory_order_relaxed);
    do {
        node->Next = expected;
    } while (!Head_.compare_exchange_weak(expected, node));
}

template <class T>
bool TMultipleProducerSingleConsumerLockFreeStack<T>::Dequeue(T* value)
{
    auto* expected = Head_.load();
    do {
        if (!expected) {
            return false;
        }
    } while (!Head_.compare_exchange_weak(expected, expected->Next));

    *value = std::move(expected->Value);
    delete expected;
    return true;
}

template <class T>
std::vector<T> TMultipleProducerSingleConsumerLockFreeStack<T>::DequeueAll(bool reverse)
{
    std::vector<T> results;
    DequeueAll(reverse, [&results] (T& value) {
        results.push_back(std::move(value));
    });
    return results;
}

template <class T>
template <class F>
bool TMultipleProducerSingleConsumerLockFreeStack<T>::DequeueAll(bool reverse, F&& functor)
{
    auto* current = Head_.exchange(nullptr);
    if (!current) {
        return false;
    }
    if (reverse) {
        auto* next = current->Next;
        current->Next = nullptr;
        while (next) {
            auto* second = next->Next;
            next->Next = current;
            current = next;
            next = second;
        }
    }
    while (current) {
        functor(current->Value);
        auto* next = current->Next;
        delete current;
        current = next;
    }
    return true;
}

template <class T>
bool TMultipleProducerSingleConsumerLockFreeStack<T>::IsEmpty() const
{
    return !Head_.load();
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TSingleProducerSingleConsumerQueue<T>::TNode
{
    std::atomic<TNode*> Next = {nullptr};
    size_t Offset = 0;
    T Data[BufferSize];
};

template <class T>
TSingleProducerSingleConsumerQueue<T>::TSingleProducerSingleConsumerQueue()
{
    auto initialNode = new TNode();
    Head_ = initialNode;
    Tail_ = initialNode;
}

template <class T>
TSingleProducerSingleConsumerQueue<T>::~TSingleProducerSingleConsumerQueue()
{
    auto current = Head_;
    while (current) {
        auto next = current->Next.load();
        delete current;
        current = next;
    }
}

template <class T>
void TSingleProducerSingleConsumerQueue<T>::Push(T&& element)
{
    auto count = Count_.load();
    size_t position = count - Tail_->Offset;

    if (Y_UNLIKELY(position == BufferSize)) {
        auto oldTail = Tail_;
        Tail_ = new TNode();
        Tail_->Offset = count;
        oldTail->Next.store(Tail_);
        position = 0;
    }

    Tail_->Data[position] = std::move(element);
    Count_ = count + 1;
}

template <class T>
T* TSingleProducerSingleConsumerQueue<T>::Front() const
{
    if (Y_UNLIKELY(Offset_ >= CachedCount_)) {
        auto count = Count_.load();
        CachedCount_ = count;

        if (Offset_ >= count) {
            return nullptr;
        }
    }

    while (Y_UNLIKELY(Offset_ >= Head_->Offset + BufferSize)) {
        auto next = Head_->Next.load();
        YT_VERIFY(next);
        delete Head_;
        Head_ = next;
    }

    auto position = Offset_ - Head_->Offset;
    return &Head_->Data[position];
}

template <class T>
void TSingleProducerSingleConsumerQueue<T>::Pop()
{
    ++Offset_;
}

template <class T>
bool TSingleProducerSingleConsumerQueue<T>::IsEmpty() const
{
    auto count = Count_.load();
    YT_ASSERT(Offset_ <= count);
    return Offset_ == count;
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT
