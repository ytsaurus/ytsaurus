#include "mpsc_queue.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TMpscQueueBase::TMpscQueueBase()
    : Head_(&Stub_)
    , Tail_(&Stub_)
{
    Y_UNUSED(StubPadding_);
    Y_UNUSED(HeadPadding_);
    Y_UNUSED(TailPadding_);
}

TMpscQueueBase::~TMpscQueueBase()
{
    // Check that queue is empty. Derived classes must ensure that the queue is empty.
    YCHECK(Head_ == Tail_);
    YCHECK(Head_ == &Stub_);
    YCHECK(Head_.load()->Next_.load() == nullptr);
}

void TMpscQueueBase::PushImpl(TMpscQueueHook* node) noexcept
{
    node->Next_.store(nullptr, std::memory_order_release);
    auto prev = Head_.exchange(node, std::memory_order_acq_rel);
    prev->Next_.store(node, std::memory_order_release);
}

TMpscQueueHook* TMpscQueueBase::PopImpl() noexcept
{
    auto tail = Tail_;
    auto next = tail->Next_.load(std::memory_order_acquire);

    // Handle stub node.
    if (tail == &Stub_) {
        if (next == nullptr) {
            return nullptr;
        }
        Tail_ = next;
        // Save tail-recursive call by updating local variables.
        tail = next;
        next = next->Next_.load(std::memory_order_acquire);
    }

    // No producer-consumer race.
    if (next) {
        Tail_ = next;
        return tail;
    }

    auto head = Head_.load(std::memory_order_acquire);

    // Concurrent producer was blocked, bail out.
    if (tail != head) {
        return nullptr;
    }

    // Decouple (future) producers and consumer by barriering via stub node.
    PushImpl(&Stub_);
    next = tail->Next_.load(std::memory_order_acquire);

    if (next) {
        Tail_ = next;
        return tail;
    }

    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

