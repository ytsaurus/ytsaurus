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

    template <class TProtoRequest, class TBuilder>
    void BuildRequest(TProtoRequest* request, TBuilder protoItemBuilder);

    template <class TProtoResponse>
    void HandleResponse(const TProtoResponse& response);

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

    template <class TProtoRequest>
    void BuildRequest(TProtoRequest* request);

    template <class TProtoResponse, class TConsumer>
    void HandleResponse(const TProtoResponse& response, TConsumer protoItemConsumer);

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
