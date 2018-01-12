#pragma once

#include "public.h"

#include <yt/core/misc/ring_queue.h>

#include <yt/core/logging/log.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

using TMessageQueueItemId = i64;

/*
 * \note Thread affinity: any
 */
template <class TItem>
class TMessageQueueOutbox
    : public TIntrinsicRefCounted
{
public:
    explicit TMessageQueueOutbox(const NLogging::TLogger& logger);

    TMessageQueueItemId Enqueue(TItem&& item);
    template <class TItems>
    void EnqueueMany(TItems&& items);

    template <class TProtoMessage, class TBuilder>
    void BuildOutcoming(TProtoMessage* message, TBuilder protoItemBuilder);

    template <class TProtoMessage>
    void HandleStatus(const TProtoMessage& message);

private:
    const NLogging::TLogger Logger;

    TSpinLock SpinLock_;
    TRingQueue<TItem> Queue_;
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
    explicit TMessageQueueInbox(const NLogging::TLogger& logger);

    template <class TProtoMessage>
    void ReportStatus(TProtoMessage* request);

    template <class TProtoMessage, class TConsumer>
    void HandleIncoming(const TProtoMessage& message, TConsumer protoItemConsumer);

private:
    const NLogging::TLogger Logger;

    TMessageQueueItemId NextExpectedItemId_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

#define MESSAGE_QUEUE_INL_H_
#include "message_queue-inl.h"
#undef MESSAGE_QUEUE_INL_H_
