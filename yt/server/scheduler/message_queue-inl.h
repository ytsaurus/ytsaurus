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
    Queue_.emplace(std::move(item));
    return NextItemId_++;
}

template <class TItem>
template <class TItems>
void TMessageQueueOutbox<TItem>::EnqueueMany(TItems&& items)
{
    auto guard = Guard(SpinLock_);
    for (auto&& item : items) {
        Queue_.emplace(std::move(item));
        ++NextItemId_;
    }
}

template <class TItem>
template <class TProtoMessage, class TBuilder>
void TMessageQueueOutbox<TItem>::BuildOutcoming(TProtoMessage* message, TBuilder protoItemBuilder)
{
    auto guard = Guard(SpinLock_);
    auto firstItemId = FirstItemId_;
    auto lastItemId = FirstItemId_ + Queue_.size() - 1;
    message->set_first_item_id(firstItemId);
    if (Queue_.empty()) {
        return;
    }
    for (auto it = Queue_.begin(); it != Queue_.end(); Queue_.move_forward(it)) {
        protoItemBuilder(message->add_items(), *it);
    }
    guard.Release();
    LOG_DEBUG("Sending outbox items (ItemIds: %v-%v)",
        firstItemId,
        lastItemId);
}

template <class TItem>
template <class TProtoMessage>
void TMessageQueueOutbox<TItem>::HandleStatus(const TProtoMessage& message)
{
    auto guard = Guard(SpinLock_);
    auto nextExpectedItemId = message.next_expected_item_id();
    YCHECK(nextExpectedItemId <= NextItemId_);
    if (nextExpectedItemId == NextItemId_) {
        return;
    }
    if (nextExpectedItemId < FirstItemId_) {
        LOG_DEBUG("Stale outbox items confirmed (NextExpectedItemId: %v, FirstItemId: %v)",
            nextExpectedItemId,
            FirstItemId_);
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
void TMessageQueueInbox::ReportStatus(TProtoRequest* request)
{
    request->set_next_expected_item_id(NextExpectedItemId_);

    LOG_DEBUG("Expecting inbox items (NextExpectedItemId: %v)",
        NextExpectedItemId_);
}

template <class TProtoMessage, class TConsumer>
void TMessageQueueInbox::HandleIncoming(const TProtoMessage& message, TConsumer protoItemConsumer)
{
    if (message.items_size() == 0) {
        return;
    }

    auto firstConsumedItemId = -1;
    auto lastConsumedItemId = -1;
    auto itemId = message.first_item_id();
    for (const auto& protoItem : message.items()) {
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

    if (firstConsumedItemId >= 0) {
        LOG_DEBUG("Inbox items handled (ReceivedIds: %v-%v, ConsumedIds: %v-%v)",
            message.first_item_id(),
            message.first_item_id() + message.items_size() - 1,
            firstConsumedItemId,
            lastConsumedItemId);
    } else {
        LOG_DEBUG("Inbox items handled but none consumed (ReceivedIds: %v-%v)",
            message.first_item_id(),
            message.first_item_id() + message.items_size() - 1);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
