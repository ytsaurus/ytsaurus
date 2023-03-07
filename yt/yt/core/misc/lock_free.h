#pragma once

#include "public.h"

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TMultipleProducerSingleConsumerLockFreeStack
{
public:
    TMultipleProducerSingleConsumerLockFreeStack(const TMultipleProducerSingleConsumerLockFreeStack&) = delete;
    void operator=(const TMultipleProducerSingleConsumerLockFreeStack&) = delete;

    TMultipleProducerSingleConsumerLockFreeStack() = default;
    ~TMultipleProducerSingleConsumerLockFreeStack();

    void Enqueue(const T& value);
    void Enqueue(T&& value);

    bool Dequeue(T* value);
    std::vector<T> DequeueAll(bool reverse = false);
    template <class F>
    bool DequeueAll(bool reverse, F&& functor);

    bool IsEmpty() const;

private:
    struct TNode;

    std::atomic<TNode*> Head_ = {nullptr};

    void DoEnqueue(TNode* node);
};

/////////////////////////////////////////////////////////////////////////////

template <class T>
class TSingleProducerSingleConsumerQueue
{
public:
    TSingleProducerSingleConsumerQueue(const TSingleProducerSingleConsumerQueue&) = delete;
    void operator=(const TSingleProducerSingleConsumerQueue&) = delete;

    TSingleProducerSingleConsumerQueue();
    ~TSingleProducerSingleConsumerQueue();

    void Push(T&& element);

    T* Front() const;
    void Pop();

    bool IsEmpty() const;

private:
    static constexpr size_t BufferSize = 128;

    struct TNode;

    std::atomic<size_t> Count_ = {0};

    mutable TNode* Head_;
    TNode* Tail_;
    size_t Offset_ = 0;
    mutable size_t CachedCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define LOCK_FREE_INL_H_
#include "lock_free-inl.h"
#undef LOCK_FREE_INL_H_
