#pragma once

#include "common.h"

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TMultipleProducerSingleConsumerLockFreeStack
{
public:
    TMultipleProducerSingleConsumerLockFreeStack(const TMultipleProducerSingleConsumerLockFreeStack&) = delete;
    void operator=(const TMultipleProducerSingleConsumerLockFreeStack&) = delete;

    TMultipleProducerSingleConsumerLockFreeStack();
    ~TMultipleProducerSingleConsumerLockFreeStack();

    void Enqueue(const T& value);
    void Enqueue(T&& value);
    bool Dequeue(T* value);
    std::vector<T> DequeueAll(bool reverse = false);
    template <class F>
    bool DequeueAll(bool reverse, F functor);

    bool IsEmpty() const;

private:
    struct TNode;

    std::atomic<TNode*> Head;

};

/////////////////////////////////////////////////////////////////////////////

template <class T>
class TSingleProducerSingleConsumerQueue
    : private TNonCopyable
{
public:
    TSingleProducerSingleConsumerQueue();
    ~TSingleProducerSingleConsumerQueue();

    void Push(T&& element);

    bool Pop(T* element);

    bool IsEmpty() const;

private:
    static constexpr size_t BufferSize = 128;

    struct TNode;

    TNode* Head_;
    TNode* Tail_;
    std::atomic<size_t> Count_ = {0};
    size_t Offset_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define LOCK_FREE_INL_H_
#include "lock_free-inl.h"
#undef LOCK_FREE_INL_H_
