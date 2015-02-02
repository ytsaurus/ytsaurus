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
    bool Dequeue(T* value);
    std::vector<T> DequeueAll();
    bool DequeueAll(bool reverse, std::function<void(T&)> functor);

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
