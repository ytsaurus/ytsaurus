#pragma once

#ifndef MESSAGE_QUEUE_INL_H_
#error "Direct inclusion of this file is not allowed, include message_queue.h"
#endif

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

template <class TItem>
TMessageQueueOutbox<TItem>::TMessageQueueOutbox(const NLogging::TLogger& logger)
    : Logger(logger)
{ }

template <class TItem>
TMessageQueueItemId TMessageQueueOutbox<TItem>::Enqueue(TItem&& item)
{
    auto guard = Guard(SpinLock_);
    Queue_.push(std::move(item));
    return NextItemId_++;
}

template <class TItem>
template <class TProtoRequest, class TBuilder>
void TMessageQueueOutbox<TItem>::BuildRequest(TProtoRequest* request, TBuilder protoItemBuilder)
{
    auto guard = Guard(SpinLock_);
    auto firstItemId = FirstItemId_;
    auto lastItemId = FirstItemId_ + Queue_.size() - 1;
    request->set_first_item_id(firstItemId);
    if (Queue_.empty()) {
        return;
    }
    for (auto it = Queue_.begin(); it != Queue_.end(); Queue_.move_forward(it)) {
        protoItemBuilder(request->add_items(), *it);
    }
    guard.Release();
    LOG_DEBUG("Sending outbox items (ItemIds: %v-%v)",
        firstItemId,
        lastItemId);
}

template <class TItem>
template <class TProtoResponse>
void TMessageQueueOutbox<TItem>::HandleResponse(const TProtoResponse& response)
{
    auto guard = Guard(SpinLock_);
    auto nextExpectedItemId = response.next_expected_item_id();
    YCHECK(nextExpectedItemId >= FirstItemId_ && nextExpectedItemId <= NextItemId_);
    if (FirstItemId_  == nextExpectedItemId) {
        return;
    }
    auto firstConfirmedItemId = FirstItemId_;
    auto lastConfirmedItemId = FirstItemId_;
    while (FirstItemId_ < nextExpectedItemId) {
        Queue_.pop();
        ++FirstItemId_;
        ++lastConfirmedItemId;
    }
    guard.Release();
    LOG_DEBUG("Outbox items confirmed (ItemIds: %v-%v)",
        firstConfirmedItemId,
        lastConfirmedItemId);
}

////////////////////////////////////////////////////////////////////////////////

inline TMessageQueueInbox::TMessageQueueInbox(const NLogging::TLogger& logger)
    : Logger(logger)
{ }

template <class TProtoRequest>
void TMessageQueueInbox::BuildRequest(TProtoRequest* request)
{
    request->set_next_expected_item_id(NextExpectedItemId_);

    LOG_DEBUG("Expecting inbox items (NextExpectedItemId: %v)",
        NextExpectedItemId_);
}

template <class TProtoResponse, class TConsumer>
void TMessageQueueInbox::HandleResponse(const TProtoResponse& response, TConsumer protoItemConsumer)
{
    auto firstItemId = response.first_item_id();
    YCHECK(firstItemId <= NextExpectedItemId_);

    if (response.items_size() == 0) {
        return;
    }

    auto firstConsumedItemId = -1;
    auto lastConsumedItemId = -1;
    auto itemId = firstItemId;
    for (const auto& protoItem : response.items()) {
        if (itemId == NextExpectedItemId_) {
            protoItemConsumer(protoItem);
            if (firstConsumedItemId < 0) {
                firstConsumedItemId = itemId;
            }
            lastConsumedItemId = itemId;
            ++NextExpectedItemId_;
        }
        ++itemId;
    }

    LOG_DEBUG("Inbox inbox consumed (ItemIds: %v-%v)",
        firstConsumedItemId,
        lastConsumedItemId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
