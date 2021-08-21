#include "mpsc_queue.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TMpscQueueBase::TMpscQueueBase()
    : Head_(&Stub_)
    , Tail_(&Stub_)
{ }

TMpscQueueBase::~TMpscQueueBase()
{
    // Check that queue is empty. Derived classes must ensure that the queue is empty.
    YT_VERIFY(Head_ == Tail_);
    YT_VERIFY(Head_ == &Stub_);
    YT_VERIFY(!Head_.load()->Next.load());
}

void TMpscQueueBase::EnqueueImpl(TMpscQueueHook* node) noexcept
{
    node->Next.store(nullptr, std::memory_order_release);
    auto* prev = Head_.exchange(node, std::memory_order_acq_rel);
    prev->Next.store(node, std::memory_order_release);
}

TMpscQueueHook* TMpscQueueBase::TryDequeueImpl() noexcept
{
    auto* tail = Tail_;
    auto* next = tail->Next.load(std::memory_order_acquire);

    // Handle stub node.
    if (tail == &Stub_) {
        if (!next) {
            return nullptr;
        }
        Tail_ = next;
        // Save tail-recursive call by updating local variables.
        tail = next;
        next = next->Next.load(std::memory_order_acquire);
    }

    // No producer-consumer race.
    if (next) {
        Tail_ = next;
        return tail;
    }

    auto* head = Head_.load(std::memory_order_acquire);

    // Concurrent producer was blocked, bail out.
    if (tail != head) {
        return nullptr;
    }

    // Decouple (future) producers and consumer by barriering via stub node.
    EnqueueImpl(&Stub_);
    next = tail->Next.load(std::memory_order_acquire);

    if (next) {
        Tail_ = next;
        return tail;
    }

    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

