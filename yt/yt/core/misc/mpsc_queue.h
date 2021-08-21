#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////
// Multiple-producer single-consumer queue.
//
// Based on http://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue
//
////////////////////////////////////////////////////////////////////////////////

struct TMpscQueueHook
{
    std::atomic<TMpscQueueHook*> Next = nullptr;
};

class TMpscQueueBase
{
protected:
    TMpscQueueBase();
    ~TMpscQueueBase();

    //! Pushes the (detached) node to the queue.
    //! Node ownership is transferred to the queue.
    void EnqueueImpl(TMpscQueueHook* node) noexcept;

    //! Pops and detaches a node from the queue. Node ownership is transferred to the caller.
    //! When null is returned then the queue is either empty or blocked.
    //! This method does not distinguish between these two cases.
    TMpscQueueHook* TryDequeueImpl() noexcept;

private:
    alignas(CacheLineSize) TMpscQueueHook Stub_;

    //! Producer-side.
    alignas(CacheLineSize) std::atomic<TMpscQueueHook*> Head_;

    //! Consumer-side.
    alignas(CacheLineSize) TMpscQueueHook* Tail_;
};

template <class T, TMpscQueueHook T::*Hook>
class TIntrusiveMpscQueue
    : public TMpscQueueBase
{
public:
    TIntrusiveMpscQueue() = default;
    TIntrusiveMpscQueue(const TIntrusiveMpscQueue&) = delete;
    TIntrusiveMpscQueue(TIntrusiveMpscQueue&&) = delete;

    ~TIntrusiveMpscQueue();

    void Enqueue(std::unique_ptr<T> node);
    std::unique_ptr<T> TryDequeue();

private:
    static TMpscQueueHook* HookFromNode(T* node) noexcept;
    static T* NodeFromHook(TMpscQueueHook* hook) noexcept;
};

template <class T>
class TMpscQueue
{
public:
    TMpscQueue() = default;
    TMpscQueue(const TMpscQueue&) = delete;
    TMpscQueue(TMpscQueue&&) = delete;

    void Enqueue(T&& value);
    bool TryDequeue(T* value);

private:
    struct TNode
    {
        explicit TNode(T&& value);

        T Value;
        TMpscQueueHook Hook;
    };

    TIntrusiveMpscQueue<TNode, &TNode::Hook> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define MPSC_QUEUE_INL_H_
#include "mpsc_queue-inl.h"
#undef MPSC_QUEUE_INL_H_

