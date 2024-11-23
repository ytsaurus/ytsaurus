#ifndef CHUNK_SCANNER_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_scanner.h"
// For the sake of sane code completion.
#include "chunk_scanner.h"
#endif

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

template <class TPayload>
void TChunkScanQueueWithPayload<TPayload>::Clear()
{
    // Since queue may be huge, destruction is offloaded into separate thread.
    // Otherwise, queue is not changed.
    auto doClear = [] <class T> (std::queue<T>& queue_) {
        std::queue<T> queue;
        std::swap(queue_, queue);
        NRpc::TDispatcher::Get()->GetHeavyInvoker()->Invoke(
            BIND([queue = std::move(queue)] { Y_UNUSED(queue); }));
    };

    doClear(Queue_);
    doClear(DelayedQueue_);
}

template <class TPayload>
bool TChunkScanQueueWithPayload<TPayload>::EnqueueChunk(TQueuedChunk chunk, std::optional<TCpuDuration> delay)
{
    if (GetScanFlag(GetChunk(chunk))) {
        return false;
    }
    SetScanFlag(GetChunk(chunk));

    auto instant = GetCpuInstant();
    RequeueDelayedChunks(instant);

    TQueueEntry queueEntry;
    if constexpr (WithPayload) {
        queueEntry = {
            .Chunk = NObjectServer::TEphemeralObjectPtr<TChunk>(chunk.Chunk),
            .Payload = std::move(chunk.Payload),
            .Instant = instant,
        };
    } else {
        queueEntry = {
            .Chunk = NObjectServer::TEphemeralObjectPtr<TChunk>(chunk),
            .Instant = instant,
        };
    }

    if (!delay) {
        Queue_.push(std::move(queueEntry));
    } else {
        delay = std::min(*delay, MaxEnqueueChunkDelay_);
        DelayedQueue_.push({
            .QueueEntry = std::move(queueEntry),
            .Deadline = instant + *delay,
        });
    }

    return true;
}

template <class TPayload>
auto TChunkScanQueueWithPayload<TPayload>::DequeueChunk() -> TQueuedChunk
{
    RequeueDelayedChunks(GetCpuInstant());

    if (Queue_.empty()) {
        return None();
    }

    TQueuedChunk front;
    TChunk* chunk;
    if constexpr (WithPayload) {
        front = {
            .Chunk = Queue_.front().Chunk.Get(),
            .Payload = std::move(Queue_.front().Payload)
        };
        chunk = front.Chunk;
    } else {
        front = Queue_.front().Chunk.Get();
        chunk = front;
    }

    if (!IsObjectAlive(chunk)) {
        Queue_.pop();
        return None();
    }

    Queue_.pop();
    ClearScanFlag(chunk);
    return front;
}

template <class TPayload>
void TChunkScanQueueWithPayload<TPayload>::RequeueDelayedChunks(NProfiling::TCpuInstant deadline)
{
    while (!DelayedQueue_.empty() && DelayedQueue_.front().Deadline < deadline) {
        auto queueEntry = std::move(DelayedQueue_.front().QueueEntry);
        queueEntry.Instant = DelayedQueue_.front().Deadline;
        DelayedQueue_.pop();
        Queue_.push(std::move(queueEntry));
    }
}

template <class TPayload>
bool TChunkScanQueueWithPayload<TPayload>::HasUnscannedChunk(NProfiling::TCpuInstant deadline) const
{
    if (!Queue_.empty()) {
        return Queue_.front().Instant < deadline;
    }

    if (!DelayedQueue_.empty()) {
        return DelayedQueue_.front().Deadline < deadline;
    }

    return false;
}

template <class TPayload>
int TChunkScanQueueWithPayload<TPayload>::GetQueueSize() const
{
    return std::ssize(Queue_) + std::ssize(DelayedQueue_);
}

template <class TPayload>
constexpr auto TChunkScanQueueWithPayload<TPayload>::None() noexcept -> TQueuedChunk
{
    return WithoutPayload(nullptr);
}

template <class TPayload>
constexpr auto TChunkScanQueueWithPayload<TPayload>::WithoutPayload(TChunk* chunk) noexcept -> TQueuedChunk
{
    if constexpr (WithPayload) {
        return {chunk, TPayload{}};
    } else {
        return chunk;
    }
}

template <class TPayload>
constexpr TChunk* TChunkScanQueueWithPayload<TPayload>::GetChunk(const TQueuedChunk& chunk) noexcept
{
    if constexpr (WithPayload) {
        return chunk.Chunk;
    } else {
        return chunk;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TPayload>
TChunkScannerWithPayload<TPayload>::TChunkScannerWithPayload(
    EChunkScanKind kind,
    bool journal)
    : TBase(kind, journal)
    , TChunkQueue(kind)
{ }

template <class TPayload>
void TChunkScannerWithPayload<TPayload>::Stop(int shardIndex)
{
    TBase::Stop(shardIndex);

    // If there are no more active shards, we clear the queue to drop all the
    // ephemeral references. Note that queue now may contain chunks
    // from non-active shards. We have to properly handle them during
    // chunk dequeueing.
    if (ActiveShardIndices_.none()) {
        TChunkQueue::Clear();
    }
}

template <class TPayload>
bool TChunkScannerWithPayload<TPayload>::EnqueueChunk(TQueuedChunk chunk, std::optional<TCpuDuration> delay)
{
    if (!TBase::IsRelevant(TChunkQueue::GetChunk(chunk))) {
        return false;
    }

    return TChunkQueue::EnqueueChunk(std::move(chunk), delay);
}

template <class TPayload>
auto TChunkScannerWithPayload<TPayload>::DequeueChunk() -> TQueuedChunk
{
    if (TBase::HasUnscannedChunk()) {
        return TChunkQueue::WithoutPayload(TGlobalChunkScanner::DequeueChunk());
    }

    auto front = TChunkQueue::DequeueChunk();
    auto* chunk = TChunkQueue::GetChunk(front);
    if (!IsObjectAlive(chunk) || !TBase::IsRelevant(chunk)) {
        return TChunkQueue::None();
    }

    return front;
}

template <class TPayload>
bool TChunkScannerWithPayload<TPayload>::HasUnscannedChunk(NProfiling::TCpuInstant deadline) const
{
    if (TBase::HasUnscannedChunk(deadline)) {
        return true;
    }

    return TChunkQueue::HasUnscannedChunk(deadline);
}

template <class TPayload>
int TChunkScannerWithPayload<TPayload>::GetQueueSize() const
{
    return TBase::GetQueueSize() + TChunkQueue::GetQueueSize();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
