#pragma once

#include <yt/core/misc/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// Mpsc queue.
/*
 * Based on http://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue
 */

struct TMpscQueueHook
{
    std::atomic<TMpscQueueHook*> Next_ = {nullptr};
};

class TMpscQueueBase
{
protected:
    TMpscQueueBase();
    ~TMpscQueueBase();

    // Pushes the (detached) node to the queue. Node ownership is transferred to the queue.
    void PushImpl(TMpscQueueHook* node) noexcept;

    // Pops and detachs a node from the queue. Node ownership is transferred to the caller.
    // When nullptr is returned that means that either queue is empty or queue is blocked.
    // This method does not distinguishes between these two cases.
    TMpscQueueHook* PopImpl() noexcept;

private:
    TMpscQueueHook Stub_;
    char StubPadding_[64 - sizeof(Stub_)];

    // Producer-side.
    std::atomic<TMpscQueueHook*> Head_;
    char HeadPadding_[64 - sizeof(Head_)];

    // Consumer-side.
    TMpscQueueHook* Tail_;
    char TailPadding_[64 - sizeof(Tail_)];
};

template <class T, TMpscQueueHook T::*M>
class TMpscQueue
    : public TMpscQueueBase
{
private:
    static TMpscQueueHook* MemberFromNode(T* node) noexcept
    {
        if (!node) {
            return nullptr;
        }
        return &(node->*M);
    }

    static T* NodeFromMember(TMpscQueueHook* member) noexcept
    {
        if (!member) {
            return nullptr;
        }
        const T* const fakeNode = nullptr;
        const char* const fakeMember = static_cast<const char*>(static_cast<const void*>(&(fakeNode->*M)));
        const size_t offset = fakeMember - static_cast<const char*>(static_cast<const void*>(fakeNode));
        return static_cast<T*>(static_cast<void*>(static_cast<char*>(static_cast<void*>(member)) - offset));
    }

public:
    void Push(std::unique_ptr<T> node)
    {
        PushImpl(MemberFromNode(node.release()));
    }

    std::unique_ptr<T> Pop()
    {
        return std::unique_ptr<T>{NodeFromMember(PopImpl())};
    }

    TMpscQueue() = default;
    TMpscQueue(const TMpscQueue&) = delete;
    TMpscQueue(TMpscQueue&&) = delete;

    ~TMpscQueue()
    {
        while (auto item = Pop());
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

