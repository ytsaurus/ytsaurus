#include "input_buffer_detail.h"

#include <yt/yt/flow/library/cpp/buffers/epoch_cycle_tracker.h>
#include <yt/yt/flow/library/cpp/buffers/offered_rate_estimator.h>

#include <yt/yt/flow/library/cpp/misc/prefetch.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/serialized_invoker.h>

namespace NYT::NFlow::NWorker {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr size_t HeapArity = 4;
static_assert(HeapArity >= 2, "HeapArity must be at least 2");

// A single 128-bit comparison compiles to branch-free cmp/sbb, unlike the
// short-circuiting (timestamp, seqno) pair comparison.
using TPackedKey = unsigned __int128;

TPackedKey PackOrderedMessageKey(const TInputBuffer::TOrderedMessage& message)
{
    return static_cast<TPackedKey>(message.AlignmentTimestamp.Underlying()) << 64 | message.SeqNo;
}

} // namespace

bool TInputBuffer::TOrderedMessage::operator<(const TOrderedMessage& right) const
{
    return PackOrderedMessageKey(*this) < PackOrderedMessageKey(right);
}

////////////////////////////////////////////////////////////////////////////////

bool TInputBuffer::TMessagesPriorityQueue::empty() const
{
    return Heap_.empty();
}

size_t TInputBuffer::TMessagesPriorityQueue::size() const
{
    return Heap_.size();
}

const TInputBuffer::TOrderedMessage& TInputBuffer::TMessagesPriorityQueue::front() const
{
    YT_ASSERT(!Heap_.empty());
    return Heap_.front();
}

void TInputBuffer::TMessagesPriorityQueue::push(TOrderedMessage&& message)
{
    auto key = PackOrderedMessageKey(message);
    Heap_.push_back(std::move(message));
    HighWatermark_ = std::max(HighWatermark_, Heap_.size());
    size_t index = Heap_.size() - 1;
    if (index == 0 || PackOrderedMessageKey(Heap_[(index - 1) / HeapArity]) <= key) {
        return;
    }
    auto item = std::move(Heap_[index]);
    do {
        size_t parent = (index - 1) / HeapArity;
        Heap_[index] = std::move(Heap_[parent]);
        index = parent;
    } while (index > 0 && key < PackOrderedMessageKey(Heap_[(index - 1) / HeapArity]));
    Heap_[index] = std::move(item);
}

TInputBuffer::TOrderedMessage TInputBuffer::TMessagesPriorityQueue::extract_front()
{
    YT_ASSERT(!Heap_.empty());
    auto result = std::move(Heap_.front());
    auto last = std::move(Heap_.back());
    Heap_.pop_back();

    const size_t size = Heap_.size();
    if (size > 0) {
        const auto lastKey = PackOrderedMessageKey(last);
        size_t index = 0;
        while (true) {
            size_t firstChild = index * HeapArity + 1;
            // |minChild| stays 0 (the root, never a child) when no child beats |last|.
            size_t minChild = 0;
            TPackedKey minKey = lastKey;
            auto considerChild = [&] (size_t child) {
                auto key = PackOrderedMessageKey(Heap_[child]);
                bool less = key < minKey;
                minChild = less ? child : minChild;
                minKey = less ? key : minKey;
            };
            if (Y_LIKELY(firstChild + HeapArity <= size)) {
                for (size_t offset = 0; offset < HeapArity; ++offset) {
                    considerChild(firstChild + offset);
                }
            } else {
                for (size_t child = firstChild; child < size; ++child) {
                    considerChild(child);
                }
            }
            if (minChild == 0) {
                break;
            }
            Heap_[index] = std::move(Heap_[minChild]);
            index = minChild;
        }
        Heap_[index] = std::move(last);
    }

    // A backlog spike must not pin the vector's memory forever. Shrinking right
    // when size drops below capacity/4 would thrash on routine fill/drain cycles,
    // so track the size high watermark instead and reallocate down only when a
    // whole check window (~capacity/4 extractions, which keeps the cost amortized
    // O(1)) passed far below the capacity.
    constexpr size_t minShrinkCapacity = 1024;
    if (4 * ++ExtractionsSinceShrinkCheck_ >= Heap_.capacity() + minShrinkCapacity) {
        if (Heap_.capacity() >= minShrinkCapacity && Heap_.capacity() >= 4 * HighWatermark_) {
            std::vector<TOrderedMessage> shrunk;
            shrunk.reserve(2 * HighWatermark_);
            shrunk.assign(std::make_move_iterator(Heap_.begin()), std::make_move_iterator(Heap_.end()));
            Heap_ = std::move(shrunk);
        }
        HighWatermark_ = Heap_.size();
        ExtractionsSinceShrinkCheck_ = 0;
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TInputBuffer::TConnectionState::Acquire(i64 inflatedSize)
{
    FreshOffer = false;
    InflatedByteLimit -= inflatedSize;
    while (!Offer.empty()) {
        if (Offer.back().second <= inflatedSize) {
            inflatedSize -= Offer.back().second;
            Offer.pop_back();
        } else {
            Offer.back().second -= inflatedSize;
            break;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TInputBuffer::TInputBuffer(
    TJobId jobId,
    NFlow::TStreamLimitUsageStateMap streamLimitUsageStates,
    NFlow::TEpochCycleTrackerPtr epochCycleTracker,
    THashMap<TStreamId, NFlow::TOfferedRateEstimatorPtr> offeredRateEstimators,
    TComputationSpecPtr computationSpec,
    TComputationId computationId,
    TDynamicComputationSpecPtr dynamicSpec,
    IInvokerPtr finalizerPoolInvoker,
    NProfiling::TProfiler profiler,
    std::function<TInstant()> timeProvider)
    : JobId_(jobId)
    , OrderingSpec_(computationSpec->InputOrdering)
    , ComputationId_(std::move(computationId))
    , FinalizerPoolInvoker_(finalizerPoolInvoker)
    , SerializedInvoker_(CreateSerializedInvoker(finalizerPoolInvoker, "InputBuffer"))
    , BatchLimiter_(dynamicSpec->MaxRowsPerBatch, dynamicSpec->MaxBytesPerBatch)
    , BatchDuration_(dynamicSpec->BatchDuration)
    , EpochCycleTracker_(std::move(epochCycleTracker))
    , TimeProvider_(std::move(timeProvider))
    , MessageProcessingTimer_(profiler.Timer("/message_processing_time"))
{
    const auto now = TimeProvider_();
    for (const auto& streamId : computationSpec->InputStreamIds) {
        auto streamProfiler = profiler
            .WithPrefix("/input_streams")
            .WithTag("stream_id", streamId.Underlying());
        auto& streamState = StreamStates_[streamId];
        streamState.OfferedMessagesRate.Update(0, now);
        streamState.OfferedBytesRate.Update(0, now);
        streamState.PersistedMessagesRate.Update(0, now);
        streamState.PersistedBytesRate.Update(0, now);
        streamState.PersistedMessagesCounter = streamProfiler.Counter("/persisted_count");
        streamState.PersistedBytesCounter = streamProfiler.Counter("/persisted_bytes");
        streamState.NotPersistedMessageGauge = streamProfiler.Gauge("/input_buffer_not_persisted_message_count");
        auto it = streamLimitUsageStates.find(streamId);
        YT_VERIFY(it != streamLimitUsageStates.end());
        streamState.LimitUsageState = std::move(it->second);
        if (auto estimatorIt = offeredRateEstimators.find(streamId);
            estimatorIt != offeredRateEstimators.end() && estimatorIt->second)
        {
            streamState.OfferedRateEstimator = std::move(estimatorIt->second);
        }
    }
}

TInputBuffer::~TInputBuffer() noexcept
{
    FinalizerPoolInvoker_->Invoke(BIND([messageStates = std::move(MessageStatesMap_), streamStates = std::move(StreamStates_)] () mutable {
        messageStates.clear();
        streamStates.clear();
    }));
}

void TInputBuffer::Reconfigure(TDynamicComputationSpecPtr dynamicSpec)
{
    SerializedInvoker_->Invoke(
        BIND(&TInputBuffer::DoReconfigure, MakeStrong(this), std::move(dynamicSpec)));
}

void TInputBuffer::DoReconfigure(TDynamicComputationSpecPtr dynamicSpec)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);
    BatchLimiter_ = TMessageBatchLimiter(dynamicSpec->MaxRowsPerBatch, dynamicSpec->MaxBytesPerBatch);
    BatchDuration_ = dynamicSpec->BatchDuration;
}

void TInputBuffer::UpdateMessageTransferingInfo(TMessageTransferingInfoPtr messageTransferingInfo)
{
    SerializedInvoker_->Invoke(
        BIND(&TInputBuffer::DoUpdateMessageTransferingInfo, MakeStrong(this), std::move(messageTransferingInfo)));
}

void TInputBuffer::DoUpdateMessageTransferingInfo(TMessageTransferingInfoPtr messageTransferingInfo)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);
    MessageTransferingInfo_ = std::move(messageTransferingInfo);
}

TFuture<std::vector<EMessageDeliveryState>> TInputBuffer::AddMessages(
    TGuid connectionId,
    std::vector<TInputMessageConstPtr> messages,
    TOnProcessedCallback onProcessed)
{
    auto now = TimeProvider_();
    return BIND(&TInputBuffer::DoAddMessages, MakeStrong(this), connectionId, Passed(std::move(messages)), Passed(std::move(onProcessed)), now)
        .AsyncVia(SerializedInvoker_)
        .Run();
}

std::vector<EMessageDeliveryState> TInputBuffer::DoAddMessages(
    TGuid connectionId,
    std::vector<TInputMessageConstPtr> messages,
    TOnProcessedCallback onProcessed,
    TInstant now)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);
    std::vector<EMessageDeliveryState> deliveryStates;
    deliveryStates.reserve(messages.size());
    THashSet<TStreamId> congestionDeclinedStreams;

    struct TAcceptedStreamCounters
    {
        i64 InflatedBytes = 0;
        i64 MaxAlignmentTimestamp = 0;
        i64 OfferedCount = 0;
        i64 OfferedBytes = 0;
    };

    THashMap<TStreamId, TAcceptedStreamCounters> acceptedByStream;

    MakePrefetcher()
        .Add([] (const TInputMessageConstPtr& message) {
            Y_PREFETCH_READ(message.Get(), 3);
        })
        .Add([] (const TInputMessageConstPtr& message) {
            message->MessageId.Prefetch();
        })
        .Add([this] (const TInputMessageConstPtr& message) {
            MessageStatesMap_.prefetch(message->MessageId);
        })
        .ForEach(messages, [&] (TInputMessageConstPtr& message) {
            TryFulfillPendingFetchCheckpoint();
            auto messageStateIt = MessageStatesMap_.find(message->MessageId);
            if (messageStateIt != MessageStatesMap_.end()) {
                deliveryStates.push_back(messageStateIt->second.CurrentDeliveryState);
                messageStateIt->second.Subscribers.push_back(onProcessed);
                return;
            }

            if (congestionDeclinedStreams.contains(message->StreamId)) {
                deliveryStates.push_back(EMessageDeliveryState::CongestionDeclined);
                return;
            }

            auto& streamState = GetOrCrash(StreamStates_, message->StreamId);
            auto& streamConnectionState = streamState.ConnectionStates[connectionId];

            // Pre-accept check: a stream with room admits one message even if it alone exceeds the limit.
            if (!streamState.LimitUsageState->IsUsageWithinLimits(streamState.Usage)) {
                congestionDeclinedStreams.insert(message->StreamId);
                deliveryStates.push_back(EMessageDeliveryState::CongestionDeclined);
                return;
            }
            streamConnectionState.Acquire(InflatedByteSize(message->ByteSize));
            streamState.Usage.CumulativeByteIn += message->ByteSize;
            ++streamState.Usage.CumulativeCountIn;

            auto& accepted = acceptedByStream[message->StreamId];
            accepted.InflatedBytes += InflatedByteSize(message->ByteSize);
            accepted.MaxAlignmentTimestamp = std::max<i64>(
                accepted.MaxAlignmentTimestamp,
                message->AlignmentTimestamp.Underlying());

            messageStateIt = EmplaceOrCrash(
                MessageStatesMap_,
                message->MessageId,
                TMessageState{
                    .StreamId = message->StreamId,
                    .ByteSize = message->ByteSize,
                    .CurrentDeliveryState = EMessageDeliveryState::Accepted,
                    .Subscribers = {onProcessed},
                    .RegisterTime = now,
                });

            streamState.Messages.push(TOrderedMessage{
                .AlignmentTimestamp = message->AlignmentTimestamp,
                .SeqNo = NextSeqNo_++,
                .Message = std::move(message),
            });
            ++accepted.OfferedCount;
            accepted.OfferedBytes += messageStateIt->second.ByteSize;
            streamState.ReadyByteSize += messageStateIt->second.ByteSize;

            ++streamState.NotPersistedMessageCount;
            streamState.NotPersistedByteSize += messageStateIt->second.ByteSize;
            streamState.NotPersistedMessageGauge.Update(streamState.NotPersistedMessageCount);

            deliveryStates.push_back(EMessageDeliveryState::Accepted);
        });

    YT_VERIFY(deliveryStates.size() == messages.size());

    for (const auto& [streamId, accepted] : acceptedByStream) {
        auto& streamState = GetOrCrash(StreamStates_, streamId);
        streamState.OfferedRateEstimator->RecordAccepted(
            accepted.InflatedBytes,
            accepted.MaxAlignmentTimestamp);
        streamState.OfferedMessagesRate.Inc(accepted.OfferedCount, now);
        streamState.OfferedBytesRate.Inc(accepted.OfferedBytes, now);
    }

    FulfillPendingFetch();

    for (auto& [streamId, streamState] : StreamStates_) {
        streamState.LimitUsageState->Update(streamState.Usage);
    }

    return deliveryStates;
}

void TInputBuffer::AddConnectionOffer(TGuid connectionId, TConnectionOffer offer)
{
    SerializedInvoker_->Invoke(
        BIND(&TInputBuffer::DoAddConnectionOffer, MakeStrong(this), connectionId, Passed(std::move(offer))));
}

void TInputBuffer::DoAddConnectionOffer(TGuid connectionId, TConnectionOffer offer)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);
    for (auto& [streamId, streamOffer] : offer) {
        auto streamStateIt = StreamStates_.find(streamId);
        if (streamStateIt == StreamStates_.end()) {
            continue;
        }
        auto& streamState = streamStateIt->second;

        {
            auto& connectionState = streamState.ConnectionStates[connectionId];
            connectionState.Offer = std::move(streamOffer);
            connectionState.FreshOffer = true;
            connectionState.UpdateEpoch = streamState.Epoch;
        }

        // Just double function amortized complexity. A manager-side limit change bypasses the
        // amortization so the raised limit turns into grants on the first offer, not the Nth;
        // the bypass must not advance the connection-GC epoch: the recomputed limit jitters on
        // every manage tick, and the GC horizon must stay measured in offer rounds.
        const bool offerRoundElapsed = --streamState.RecalculateCounter <= 0;
        if (offerRoundElapsed ||
            streamState.LimitUsageState->GetLimitBytes() != streamState.LastRecalculatedLimitBytes)
        {
            RecalculateStreamLimits(streamState, /*collectStaleConnections*/ offerRoundElapsed);
            streamState.Usage.PendingInflatedBytes = GetPendingSize(streamState);
            streamState.LimitUsageState->Update(streamState.Usage);
        }
    }
}

TFuture<THashMap<TStreamId, i64>> TInputBuffer::GetConnectionLimits(TGuid connectionId)
{
    return BIND(&TInputBuffer::DoGetConnectionLimits, MakeStrong(this), connectionId)
        .AsyncVia(SerializedInvoker_)
        .Run();
}

THashMap<TStreamId, i64> TInputBuffer::DoGetConnectionLimits(TGuid connectionId)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);
    THashMap<TStreamId, i64> result;
    for (const auto& [streamId, streamState] : StreamStates_) {
        auto it = streamState.ConnectionStates.find(connectionId);
        if (it != streamState.ConnectionStates.end() && it->second.InflatedByteLimit > 0) {
            result[streamId] = it->second.InflatedByteLimit;
        }
    }
    return result;
}

void TInputBuffer::MarkPersisted(std::deque<TMessageId> messageIds)
{
    auto now = TimeProvider_();
    SerializedInvoker_->Invoke(
        BIND(
            &TInputBuffer::DoAcknowledge,
            MakeStrong(this),
            Passed(std::move(messageIds)),
            /*reportProcessed*/ true,
            now));
}

void TInputBuffer::MarkDeduplicated(std::deque<TMessageId> messageIds)
{
    auto now = TimeProvider_();
    SerializedInvoker_->Invoke(
        BIND(
            &TInputBuffer::DoAcknowledge,
            MakeStrong(this),
            Passed(std::move(messageIds)),
            /*reportProcessed*/ false,
            now));
}

namespace {

struct TOnProcessedCallbackHash
{
    size_t operator()(const IInputBuffer::TOnProcessedCallback& callback) const
    {
        return THash<void*>()(callback.GetHandle());
    }
};

} // namespace

void TInputBuffer::DoAcknowledge(
    std::deque<TMessageId> messageIds,
    bool reportProcessed,
    TInstant now)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);
    absl::flat_hash_map<
        TOnProcessedCallback,
        std::vector<TMessageId>,
        TOnProcessedCallbackHash>
        messageIdsByCallback;

    MakePrefetcher()
        .Add([] (const TMessageId& messageId) {
            messageId.Prefetch();
        })
        .Add([this] (const TMessageId& messageId) {
            MessageStatesMap_.prefetch(messageId);
        })
        .ForEach(messageIds, [&] (const TMessageId& messageId) {
            TryFulfillPendingFetchCheckpoint();
            auto it = GetIteratorOrCrash(MessageStatesMap_, messageId);
            auto& messageState = it->second;

            for (auto& callback : messageState.Subscribers) {
                messageIdsByCallback.try_emplace(std::move(callback)).first->second.push_back(messageId);
            }
            auto& streamState = GetOrCrash(StreamStates_, messageState.StreamId);
            if (reportProcessed) {
                MessageProcessingTimer_.Record(now - messageState.RegisterTime);
                streamState.PersistedMessagesCounter.Increment(1);
                streamState.PersistedBytesCounter.Increment(messageState.ByteSize);
                streamState.PersistedMessagesRate.Inc(1, now);
                streamState.PersistedBytesRate.Inc(messageState.ByteSize, now);
            }

            --streamState.NotPersistedMessageCount;
            streamState.NotPersistedByteSize -= messageState.ByteSize;
            streamState.NotPersistedMessageGauge.Update(streamState.NotPersistedMessageCount);

            MessageStatesMap_.erase(it);
        });

    FinalizerPoolInvoker_->Invoke(BIND([jobId = JobId_, messageIdsByCallback = std::move(messageIdsByCallback)] () mutable {
        for (auto& [callback, callbackMessageIds] : messageIdsByCallback) {
            callback(jobId, std::move(callbackMessageIds));
        }
    }));
}

TFuture<THashMap<TStreamId, TInflightMetricsPtr>> TInputBuffer::GetInflightMetrics()
{
    return BIND(&TInputBuffer::DoGetInflightMetrics, MakeStrong(this))
        .AsyncVia(SerializedInvoker_)
        .Run();
}

THashMap<TStreamId, TInflightMetricsPtr> TInputBuffer::DoGetInflightMetrics() const
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);

    const auto now = TimeProvider_();
    THashMap<TStreamId, TInflightMetricsPtr> result;
    for (const auto& [streamId, streamState] : StreamStates_) {
        auto metrics = New<TInflightMetrics>();
        metrics->Count = streamState.NotPersistedMessageCount;
        metrics->ByteSize = streamState.NotPersistedByteSize;
        metrics->ReadyCount = std::ssize(streamState.Messages);
        metrics->ReadyByteSize = streamState.ReadyByteSize;
        metrics->OfferedCountPerSec = streamState.OfferedMessagesRate.GetRate(now);
        metrics->OfferedBytesPerSec = streamState.OfferedBytesRate.GetRate(now);

        metrics->ProcessedCountPerSec = streamState.PersistedMessagesRate.GetRate(now);
        metrics->ProcessedBytesPerSec = streamState.PersistedBytesRate.GetRate(now);
        result.emplace(streamId, std::move(metrics));
    }
    return result;
}

double TInputBuffer::ComputeStreamBias(TStreamId streamId, const TInputMessageConstPtr& frontMessage) const
{
    auto statistics = GetOrDefault(MessageTransferingInfo_->StreamTimestampStatistics, streamId);
    if (statistics.MessageCount == 0) {
        statistics.RegisterMessage(TTimestampStatistics::ComputeRegistrationInfo(*frontMessage));
    }
    return ComputeOrderingTimestampBias(streamId, statistics, OrderingSpec_);
}

TFuture<std::vector<TInputMessageConstPtr>> TInputBuffer::GetInputBatch(const THashSet<TStreamId>& allowedStreams)
{
    auto now = TInstant::Now();
    auto deadline = NotFullBatchDeadline_.load();
    if (now < deadline) {
        return TDelayedExecutor::MakeDelayed(deadline - now)
            .Apply(BIND(&TInputBuffer::PublishPendingFetch, MakeStrong(this), allowedStreams));
    }
    return PublishPendingFetch(allowedStreams);
}

TFuture<std::vector<TInputMessageConstPtr>> TInputBuffer::PublishPendingFetch(THashSet<TStreamId> allowedStreams)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto pendingFetch = New<TPendingFetch>();
    pendingFetch->Promise = NewPromise<std::vector<TInputMessageConstPtr>>();
    pendingFetch->AllowedStreams = std::move(allowedStreams);
    auto future = pendingFetch->Promise.ToFuture();

    auto previous = PendingFetch_.Exchange(pendingFetch);
    YT_VERIFY(!previous);

    SerializedInvoker_->Invoke(BIND(&TInputBuffer::FulfillPendingFetch, MakeStrong(this)));

    return future;
}

void TInputBuffer::FulfillPendingFetch()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);

    auto pendingFetch = PendingFetch_.Exchange(TPendingFetchPtr());
    if (!pendingFetch) {
        // The add path has already served it.
        return;
    }
    pendingFetch->Promise.Set(ExtractBatch(pendingFetch->AllowedStreams));
}

void TInputBuffer::TryFulfillPendingFetchCheckpoint()
{
    // Deliberately no affinity assert before the counter: this is called per message on the
    // hot insert/persist loops and must stay branch-cheap.
    if ((FulfillCheckpointCounter_++ % FulfillCheckpointPeriod) != 0) {
        return;
    }
    FulfillPendingFetch();
}

std::vector<TInputMessageConstPtr> TInputBuffer::ExtractBatch(const THashSet<TStreamId>& allowedStreams)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);

    using TPriority = std::pair<TSystemTimestamp, ui64>;
    std::vector<std::pair<TMessagesPriorityQueue*, std::function<TPriority()>>> queues;
    std::vector<TStreamState*> extractionStreamStates;
    size_t queuedMessageCount = 0;
    for (auto& [streamId, streamState] : StreamStates_) {
        if (!streamState.Messages.empty() && allowedStreams.contains(streamId)) {
            auto bias = ComputeStreamBias(streamId, streamState.Messages.front().Message);
            queues.emplace_back(&streamState.Messages, [bias, messagesPtr = &streamState.Messages] () -> TPriority {
                YT_ASSERT(!messagesPtr->empty());
                const auto& front = messagesPtr->front();
                return {TSystemTimestamp(front.AlignmentTimestamp.Underlying() + bias), front.SeqNo};
            });
            extractionStreamStates.push_back(&streamState);
            queuedMessageCount += streamState.Messages.size();
        }
    }

    auto batchLimiter = BatchLimiter_;
    std::vector<TInputMessageConstPtr> batch;
    const size_t estimatedBatchMessageCount = std::min<size_t>(queuedMessageCount, batchLimiter.GetMaxRowsPerBatch());
    batch.reserve(estimatedBatchMessageCount);

    MergingExtractBatch(
        std::move(queues),
        [] (TOrderedMessage& message) -> TInputMessageConstPtr& {
            return message.Message;
        },
        batchLimiter,
        [&] (TOrderedMessage& orderedMessage) {
            auto& streamState = GetOrCrash(StreamStates_, orderedMessage.Message->StreamId);
            streamState.Usage.CumulativeByteOut += orderedMessage.Message->ByteSize;
            ++streamState.Usage.CumulativeCountOut;
            streamState.ReadyByteSize -= orderedMessage.Message->ByteSize;
            batch.push_back(std::move(orderedMessage.Message));
        });

    if (!batchLimiter.IsFull()) {
        NotFullBatchDeadline_.store(TInstant::Now() + BatchDuration_);
    }

    // Extraction freed buffer space; regrant connection windows right away so the next
    // PushMessages response carries fresh limits instead of waiting out the offer amortization.
    if (!batch.empty()) {
        for (auto* streamState : extractionStreamStates) {
            RecalculateStreamLimits(*streamState, /*collectStaleConnections*/ false);
            streamState->Usage.PendingInflatedBytes = GetPendingSize(*streamState);
        }
    }

    for (auto& [streamId, streamState] : StreamStates_) {
        streamState.LimitUsageState->Update(streamState.Usage);
    }

    if (!batch.empty()) {
        // The epoch estimate only raises the BDP floor (gain_epochs × demand ×
        // epoch), boxed below by the used peak and above by max_duration, so its
        // precision does not matter: record every inter-extraction interval,
        // including an idle wait, rather than gate on leftover backlog. The gate
        // used to silence latency-bound jobs that drain their whole buffer.
        auto extractionInstant = TimeProvider_();
        if (EpochCycleTracker_ && PrevBatchInstant_) {
            EpochCycleTracker_->RecordCycle(extractionInstant - *PrevBatchInstant_);
        }
        PrevBatchInstant_ = extractionInstant;
    }

    return batch;
}

void TInputBuffer::RecalculateStreamLimits(TStreamState& streamState, bool collectStaleConnections)
{
    using THeapElement = std::pair<TConnectionState*, i64>; // (it, currentBucketIndex).

    // The announced backlog plus the accepted history estimate the producer's
    // rate over its own alignment timestamps — an instant demand signal
    // independent of both the issued limit (a limit-based measurement can never
    // see demand above the limit) and the processing clock.
    {
        TCompactVector<std::pair<i64, i64>, 16> pendingBuckets;
        for (const auto& [connectionId, connectionState] : streamState.ConnectionStates) {
            for (const auto& [minOrderTimestamp, inflatedByteSize] : connectionState.Offer) {
                pendingBuckets.emplace_back(minOrderTimestamp.Underlying(), inflatedByteSize);
            }
        }
        streamState.LimitUsageState->SetOfferedInflatedBytesPerSecond(
            streamState.OfferedRateEstimator->EstimateRate(pendingBuckets));
    }

    constexpr i64 lastEpochStoreCount = 5; // TODO: something better? Configurable?

    // Cleanup.
    for (auto it = streamState.ConnectionStates.begin(); it != streamState.ConnectionStates.end();) {
        if (collectStaleConnections && it->second.UpdateEpoch + lastEpochStoreCount < streamState.Epoch) {
            streamState.ConnectionStates.erase(it++);
        } else {
            it->second.InflatedByteLimit = 0;
            ++it;
        }
    }

    std::vector<THeapElement> heapElements;
    for (auto& connectionState : streamState.ConnectionStates) {
        if (!connectionState.second.Offer.empty()) {
            heapElements.emplace_back(THeapElement{&connectionState.second, std::ssize(connectionState.second.Offer) - 1});
        }
    }
    ShuffleRange(heapElements);

    static const auto comparator = [] (const THeapElement& lhs, const THeapElement& rhs) {
        return lhs.first->Offer[lhs.second].first > rhs.first->Offer[rhs.second].first;
    };

    std::priority_queue<THeapElement, std::vector<THeapElement>, decltype(comparator)> heap(comparator, std::move(heapElements));

    // Match the manager's accounting: limit is inflated, so used must be inflated too.
    i64 inflatedUsedBytes = streamState.Usage.GetInflatedInflightBytes(streamState.LimitUsageState->GetInflationPerMessage());
    i64 inflatedLimitBytes = streamState.LimitUsageState->GetLimitBytes();
    streamState.LastRecalculatedLimitBytes = inflatedLimitBytes;
    // Do not allocate small share of buffer to reduce retransmits.
    i64 inflatedFreeBytes = std::max<i64>(inflatedLimitBytes * 0.9 - inflatedUsedBytes, 0);

    while (!heap.empty() && inflatedFreeBytes > 0) {
        auto [state, currentBucketIndex] = heap.top();
        heap.pop();

        const i64 inflatedBucketSize = state->Offer[currentBucketIndex].second;
        if (inflatedBucketSize > inflatedFreeBytes && !state->FreshOffer) {
            break;
        }

        state->InflatedByteLimit += inflatedBucketSize;
        inflatedFreeBytes -= inflatedBucketSize;

        if (currentBucketIndex > 0) {
            heap.push({state, currentBucketIndex - 1});
        }
    }

    if (heap.empty() && inflatedFreeBytes > 0 && !streamState.ConnectionStates.empty()) {
        const i64 inflatedUniformBonusLimit = inflatedFreeBytes / std::ssize(streamState.ConnectionStates);
        for (auto& [_, connectionState] : streamState.ConnectionStates) {
            connectionState.InflatedByteLimit += inflatedUniformBonusLimit;
        }
    }

    if (collectStaleConnections) {
        streamState.Epoch += 1;
        streamState.RecalculateCounter = std::ssize(streamState.ConnectionStates);
    }
}

i64 TInputBuffer::GetPendingSize(const TStreamState& streamState)
{
    i64 result = 0;
    for (auto& [connectionId, connectionState] : streamState.ConnectionStates) {
        for (auto& [systemTimestamp, inflatedByteSize] : connectionState.Offer) {
            result += inflatedByteSize;
        }
    }
    return result;
}

const TComputationId& TInputBuffer::GetComputationId()
{
    return ComputationId_;
}

TSystemTimestamp TInputBuffer::GetMinStabilizedEventTimestamp()
{
    return WaitFor(
        BIND(&TInputBuffer::DoGetMinStabilizedEventTimestamp, MakeStrong(this))
            .AsyncVia(SerializedInvoker_)
            .Run())
        .ValueOrThrow();
}

TSystemTimestamp TInputBuffer::DoGetMinStabilizedEventTimestamp()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);
    auto result = InfinitySystemTimestamp;
    for (const auto& [streamId, streamState] : StreamStates_) {
        if (streamState.Messages.empty()) {
            continue;
        }
        const auto& front = streamState.Messages.front();
        auto bias = ComputeStreamBias(streamId, front.Message);
        result = std::min(result, TSystemTimestamp(front.AlignmentTimestamp.Underlying() + bias));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
