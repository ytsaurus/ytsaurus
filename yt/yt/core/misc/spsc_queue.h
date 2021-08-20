#pragma once

#include "public.h"

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Single producer single consumer lock-free queue.
template <class T>
class TSpscQueue
{
public:
    TSpscQueue(const TSpscQueue&) = delete;
    void operator=(const TSpscQueue&) = delete;

    TSpscQueue();
    ~TSpscQueue();

    void Push(T&& element);

    T* Front() const;
    void Pop();

    bool IsEmpty() const;

private:
    static constexpr size_t BufferSize = 128;

    struct TNode;

    std::atomic<size_t> Count_ = 0;

    mutable TNode* Head_;
    TNode* Tail_;
    size_t Offset_ = 0;
    mutable size_t CachedCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define SPSC_QUEUE_INL_H_
#include "spsc_queue-inl.h"
#undef SPSC_QUEUE_INL_H_
