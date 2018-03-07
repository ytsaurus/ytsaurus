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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define LOCK_FREE_INL_H_
#include "lock_free-inl.h"
#undef LOCK_FREE_INL_H_
