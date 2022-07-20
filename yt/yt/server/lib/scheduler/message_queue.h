#pragma once

#include "public.h"

#include <yt/yt/core/misc/ring_queue.h>
#include <yt/yt/core/misc/mpsc_stack.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/library/profiling/sensor.h>

#include <type_traits>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

using TMessageQueueItemId = i64;

template <class TItem>
class TMessageQueueOutbox
    : public TRefCounted
{
public:
    TMessageQueueOutbox(
        const NLogging::TLogger& logger,
        const NProfiling::TProfiler& profiler,
        const IInvokerPtr& invoker,
        bool supportTracing = false);

    /*
     * \note Thread affinity: any
     */
    void Enqueue(TItem&& item);
    /*
     * \note Thread affinity: any
     */
    void Enqueue(std::vector<TItem>&& items);

    /*
     * \note Thread affinity: single-threaded
     */
    template <class TProtoMessage, class TBuilder>
    void BuildOutcoming(TProtoMessage* message, TBuilder protoItemBuilder);

    /*
     * \note Thread affinity: single-threaded
     */
    template <class TProtoMessage, class TBuilder>
    void BuildOutcoming(TProtoMessage* message, TBuilder protoItemBuilder, i64 itemLimit);

    /*
     * \note Thread affinity: single-threaded
     */
    template <class TProtoMessage>
    void BuildOutcoming(TProtoMessage* message);

    /*
     * \note Thread affinity: single-threaded
     */
    template <class TProtoMessage>
    void BuildOutcoming(TProtoMessage* message, i64 itemLimit);

    /*
     * \note Thread affinity: single-threaded
     */
    template <class TProtoMessage>
    void HandleStatus(const TProtoMessage& message);

private:
    const NLogging::TLogger Logger;

    NProfiling::TCounter EnqueuedItemsCounter_;
    NProfiling::TCounter HandledItemsCounter_;
    NProfiling::TGauge PendingItemsGauge_;

    const IInvokerPtr Invoker_;

    const bool SupportTracing_;

    struct TItemRequest
    {
        TItem Item;
        NTracing::TTraceContextPtr TraceContext = nullptr;
    };

    struct TItemsRequest
    {
        std::vector<TItem> Items;
        NTracing::TTraceContextPtr TraceContext = nullptr;
    };

    using TEntry = std::variant<TItemRequest, TItemsRequest>;
    TMpscStack<TEntry> Stack_;

    TRingQueue<TItemRequest> Queue_;
    TMessageQueueItemId FirstItemId_ = 0;
    TMessageQueueItemId NextItemId_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

/*
 * \note Thread affinity: single-threaded
 */
class TMessageQueueInbox
{
public:
    TMessageQueueInbox(
        const NLogging::TLogger& logger,
        const NProfiling::TProfiler& profiler,
        const IInvokerPtr& invoker);

    template <class TProtoMessage>
    void ReportStatus(TProtoMessage* request);

    template <class TProtoMessage, class TConsumer>
    void HandleIncoming(TProtoMessage* message, TConsumer protoItemConsumer);

    template <class TItem, class TProtoMessage, class TConsumer>
    void HandleIncoming(TProtoMessage* message, TConsumer protoItemConsumer);

private:
    const NLogging::TLogger Logger;

    NProfiling::TCounter HandledItemsCounter_;

    const IInvokerPtr Invoker_;

    TMessageQueueItemId NextExpectedItemId_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

#define MESSAGE_QUEUE_INL_H_
#include "message_queue-inl.h"
#undef MESSAGE_QUEUE_INL_H_
