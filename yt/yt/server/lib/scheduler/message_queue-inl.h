#ifndef MESSAGE_QUEUE_INL_H_
#error "Direct inclusion of this file is not allowed, include message_queue.h"
// For the sake of sane code completion.
#include "message_queue.h"
#endif

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

template<class T, typename = void>
struct THasProtoTracingExtension
    : std::false_type
{ };

template<class T>
struct THasProtoTracingExtension<T, decltype((void)T::tracing_ext, void())>
    : std::true_type
{ };

////////////////////////////////////////////////////////////////////////////////

template <class TItem>
TMessageQueueOutbox<TItem>::TMessageQueueOutbox(
    const NLogging::TLogger& logger,
    const NProfiling::TProfiler& profiler,
    const IInvokerPtr& invoker,
    bool supportTracing)
    : Logger(logger)
    , EnqueuedItemsCounter_(profiler.WithTag("queue_type", "outbox").Counter("/message_queue/enqueued_items"))
    , HandledItemsCounter_(profiler.WithTag("queue_type", "outbox").Counter("/message_queue/handled_items"))
    , PendingItemsGauge_(profiler.WithTag("queue_type", "outbox").Gauge("/message_queue/pending_items"))
    , Invoker_(invoker)
    , SupportTracing_(supportTracing)
{ }

template <class TItem>
void TMessageQueueOutbox<TItem>::Enqueue(TItem&& item)
{
    VERIFY_THREAD_AFFINITY_ANY();

    EnqueuedItemsCounter_.Increment();

    Stack_.Enqueue(TItemRequest{
        std::move(item),
        SupportTracing_ ? NTracing::TryGetCurrentTraceContext() : nullptr
    });
}

template <class TItem>
void TMessageQueueOutbox<TItem>::Enqueue(std::vector<TItem>&& items)
{
    VERIFY_THREAD_AFFINITY_ANY();

    EnqueuedItemsCounter_.Increment(items.size());

    Stack_.Enqueue(TItemsRequest{
        std::move(items),
        SupportTracing_ ? NTracing::TryGetCurrentTraceContext() : nullptr
    });
}

template <class TItem>
template <class TProtoMessage, class TBuilder>
void TMessageQueueOutbox<TItem>::BuildOutcoming(TProtoMessage* message, TBuilder protoItemBuilder)
{
    BuildOutcoming(message, protoItemBuilder, std::numeric_limits<i64>::max());
}

template <class TItem>
template <class TProtoMessage, class TBuilder>
void TMessageQueueOutbox<TItem>::BuildOutcoming(TProtoMessage* message, TBuilder protoItemBuilder, i64 itemCountLimit)
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    Stack_.DequeueAll(true, [&] (TEntry& entry) {
        Visit(std::move(entry),
            [&] (TItemRequest&& itemRequest) {
                Queue_.emplace(std::move(itemRequest));
                ++NextItemId_;
            },
            [&] (TItemsRequest&& itemsRequest) {
                for (auto& item : itemsRequest.Items) {
                    Queue_.emplace(TItemRequest{std::move(item), itemsRequest.TraceContext});
                    ++NextItemId_;
                }
            });
    });

    i64 itemCount = std::min(itemCountLimit, std::ssize(Queue_));

    auto firstItemId = FirstItemId_;
    auto lastItemId = FirstItemId_ + itemCount - 1;
    message->set_first_item_id(firstItemId);

    PendingItemsGauge_.Update(Queue_.size());

    if (Queue_.empty()) {
        return;
    }

    auto it = Queue_.begin();
    for (i64 iter = 0; iter < itemCount; ++iter) {
        auto* newItem = message->add_items();
        protoItemBuilder(newItem, it->Item);

        using TProtoItem = typename std::remove_reference<decltype(*newItem)>::type;
        if constexpr (THasProtoTracingExtension<TProtoItem>{}) {
            if (it->TraceContext) {
                auto* tracingExt = newItem->MutableExtension(TProtoItem::tracing_ext);
                ToProto(tracingExt, it->TraceContext);
            }
        }
        Queue_.move_forward(it);
    }
    YT_LOG_DEBUG("Sending outbox items (ItemIds: %v-%v, ItemCount: %v, RetainedCount: %v)",
        firstItemId,
        lastItemId,
        itemCount,
        Queue_.size() - itemCount);
}

template <class TItem>
template <class TProtoMessage>
void TMessageQueueOutbox<TItem>::BuildOutcoming(TProtoMessage* message)
{
    BuildOutcoming(message, std::numeric_limits<i64>::max());
}

template <class TItem>
template <class TProtoMessage>
void TMessageQueueOutbox<TItem>::BuildOutcoming(TProtoMessage* message, i64 itemCountLimit)
{
    BuildOutcoming(
        message,
        [] (auto* proto, const auto& item) {
            ToProto(proto, item);
        },
        itemCountLimit);
}

template <class TItem>
template <class TProtoMessage>
void TMessageQueueOutbox<TItem>::HandleStatus(const TProtoMessage& message)
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    auto nextExpectedItemId = message.next_expected_item_id();
    YT_VERIFY(nextExpectedItemId <= NextItemId_);
    if (nextExpectedItemId == FirstItemId_) {
        return;
    }
    if (nextExpectedItemId < FirstItemId_) {
        YT_LOG_DEBUG("Stale outbox items confirmed (NextExpectedItemId: %v, FirstItemId: %v)",
            nextExpectedItemId,
            FirstItemId_);
        return;
    }
    auto firstConfirmedItemId = FirstItemId_;
    auto lastConfirmedItemId = FirstItemId_;
    while (FirstItemId_ < nextExpectedItemId) {
        Queue_.pop();
        lastConfirmedItemId = FirstItemId_++;
    }

    HandledItemsCounter_.Increment(lastConfirmedItemId - firstConfirmedItemId);

    YT_LOG_DEBUG("Outbox items confirmed (ItemIds: %v-%v, ItemCount: %v)",
        firstConfirmedItemId,
        lastConfirmedItemId,
        lastConfirmedItemId - firstConfirmedItemId + 1);
}

////////////////////////////////////////////////////////////////////////////////

inline TMessageQueueInbox::TMessageQueueInbox(
    const NLogging::TLogger& logger,
    const NProfiling::TProfiler& profiler,
    const IInvokerPtr& invoker)
    : Logger(logger)
    , HandledItemsCounter_(profiler.WithTag("queue_type", "inbox").Counter("/message_queue/handled_items"))
    , Invoker_(invoker)
{ }

template <class TProtoRequest>
void TMessageQueueInbox::ReportStatus(TProtoRequest* request)
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    request->set_next_expected_item_id(NextExpectedItemId_);

    YT_LOG_DEBUG("Inbox status reported (NextExpectedItemId: %v)",
        NextExpectedItemId_);
}

template <class TProtoMessage, class TConsumer>
void TMessageQueueInbox::HandleIncoming(TProtoMessage* message, TConsumer protoItemConsumer)
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    if (message->items_size() == 0) {
        return;
    }

    auto firstConsumedItemId = -1;
    auto lastConsumedItemId = -1;
    auto itemId = message->first_item_id();
    for (auto& protoItem : *message->mutable_items()) {
        if (itemId == NextExpectedItemId_) {
            protoItemConsumer(&protoItem);
            if (firstConsumedItemId < 0) {
                firstConsumedItemId = itemId;
            }
            lastConsumedItemId = itemId;
            ++NextExpectedItemId_;
        }
        ++itemId;
    }

    HandledItemsCounter_.Increment(message->items_size());

    if (firstConsumedItemId >= 0) {
        YT_LOG_DEBUG("Inbox items received and consumed (ReceivedIds: %v-%v, ConsumedIds: %v-%v, ItemCount: %v)",
            message->first_item_id(),
            message->first_item_id() + message->items_size() - 1,
            firstConsumedItemId,
            lastConsumedItemId,
            message->items_size());
    } else {
        YT_LOG_DEBUG("Inbox items received but none consumed (ReceivedIds: %v-%v)",
            message->first_item_id(),
            message->first_item_id() + message->items_size() - 1);
    }
}

template <class TItem, class TProtoMessage, class TConsumer>
void TMessageQueueInbox::HandleIncoming(TProtoMessage* message, TConsumer protoItemConsumer)
{
    HandleIncoming(
        message,
        [&] (auto* proto) {
            TItem item;
            FromProto(&item, proto);
            protoItemConsumer(std::move(item));
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
