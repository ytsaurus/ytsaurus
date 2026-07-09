#include "input_buffer_detail.h"

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
    TComputationSpecPtr computationSpec,
    TComputationId computationId,
    TDynamicComputationSpecPtr dynamicSpec,
    IInvokerPtr finalizerPoolInvoker,
    NProfiling::TProfiler profiler)
    : JobId_(jobId)
    , OrderingSpec_(computationSpec->InputOrdering)
    , ComputationId_(std::move(computationId))
    , FinalizerPoolInvoker_(finalizerPoolInvoker)
    , SerializedInvoker_(CreateSerializedInvoker(finalizerPoolInvoker, "InputBuffer"))
    , BatchLimiter_(dynamicSpec->MaxRowsPerBatch, dynamicSpec->MaxBytesPerBatch)
    , BatchDuration_(dynamicSpec->BatchDuration)
    , MessageProcessingTimer_(profiler.Timer("/message_processing_time"))
{
    for (const auto& streamId : computationSpec->InputStreamIds) {
        auto streamProfiler = profiler
            .WithPrefix("/input_streams")
            .WithTag("stream_id", streamId.Underlying());
        auto& streamState = StreamStates_[streamId];
        streamState.PersistedMessagesCounter = streamProfiler.Counter("/persisted_count");
        streamState.PersistedBytesCounter = streamProfiler.Counter("/persisted_bytes");
        streamState.NotPersistedMessageGauge = streamProfiler.Gauge("/input_buffer_not_persisted_message_count");
        auto it = streamLimitUsageStates.find(streamId);
        YT_VERIFY(it != streamLimitUsageStates.end());
        streamState.LimitUsageState = std::move(it->second);
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
    auto now = TInstant::Now();
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

    for (auto& message : messages) {
        auto messageStateIt = MessageStatesMap_.find(message->MessageId);
        if (messageStateIt != MessageStatesMap_.end()) {
            deliveryStates.push_back(messageStateIt->second.CurrentDeliveryState);
            messageStateIt->second.Subscribers.push_back(onProcessed);
            continue;
        }

        if (congestionDeclinedStreams.contains(message->StreamId)) {
            deliveryStates.push_back(EMessageDeliveryState::CongestionDeclined);
            continue;
        }

        auto& streamState = GetOrCrash(StreamStates_, message->StreamId);
        auto& streamConnectionState = streamState.ConnectionStates[connectionId];

        // Pre-accept check: a stream with room admits one message even if it alone exceeds the limit.
        if (!streamState.LimitUsageState->IsUsageWithinLimits(streamState.Usage)) {
            congestionDeclinedStreams.insert(message->StreamId);
            deliveryStates.push_back(EMessageDeliveryState::CongestionDeclined);
            continue;
        }
        streamConnectionState.Acquire(InflatedByteSize(message->ByteSize));
        streamState.Usage.CumulativeByteIn += message->ByteSize;
        ++streamState.Usage.CumulativeCountIn;

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

        ++streamState.NotPersistedMessageCount;
        streamState.NotPersistedMessageGauge.Update(streamState.NotPersistedMessageCount);

        deliveryStates.push_back(EMessageDeliveryState::Accepted);
    }

    YT_VERIFY(deliveryStates.size() == messages.size());

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

        // Just double function amortized complexity.
        if (--streamState.RecalculateCounter <= 0) {
            RecalculateStreamLimits(streamState);
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
    auto now = TInstant::Now();
    SerializedInvoker_->Invoke(
        BIND(&TInputBuffer::DoMarkPersisted, MakeStrong(this), Passed(std::move(messageIds)), now));
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

void TInputBuffer::DoMarkPersisted(std::deque<TMessageId> messageIds, TInstant now)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);
    absl::flat_hash_map<
        TOnProcessedCallback,
        std::vector<TMessageId>,
        TOnProcessedCallbackHash>
        messageIdsByCallback;

    for (const auto& messageId : messageIds) {
        auto it = GetIteratorOrCrash(MessageStatesMap_, messageId);
        auto& messageState = it->second;

        for (auto& callback : messageState.Subscribers) {
            messageIdsByCallback.try_emplace(std::move(callback)).first->second.push_back(messageId);
        }
        MessageProcessingTimer_.Record(now - messageState.RegisterTime);

        auto& streamState = GetOrCrash(StreamStates_, messageState.StreamId);
        streamState.PersistedMessagesCounter.Increment(1);
        streamState.PersistedBytesCounter.Increment(messageState.ByteSize);

        --streamState.NotPersistedMessageCount;
        streamState.NotPersistedMessageGauge.Update(streamState.NotPersistedMessageCount);

        MessageStatesMap_.erase(it);
    }

    FinalizerPoolInvoker_->Invoke(BIND([jobId = JobId_, messageIdsByCallback = std::move(messageIdsByCallback)] () mutable {
        for (auto& [callback, callbackMessageIds] : messageIdsByCallback) {
            callback(jobId, std::move(callbackMessageIds));
        }
    }));
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
    return BIND(&TInputBuffer::DoGetInputBatch, MakeStrong(this), allowedStreams)
        .AsyncVia(SerializedInvoker_)
        .Run();
}

TFuture<std::vector<TInputMessageConstPtr>> TInputBuffer::DoGetInputBatch(THashSet<TStreamId> allowedStreams)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);

    auto now = TInstant::Now();

    if (now < LastNotFullBatchInstant_ + BatchDuration_) {
        auto sleepTime = LastNotFullBatchInstant_ + BatchDuration_ - now;
        return TDelayedExecutor::MakeDelayed(sleepTime, SerializedInvoker_)
            .Apply(BIND(&TInputBuffer::DoGetInputBatch, MakeStrong(this), std::move(allowedStreams)));
    }

    using TPriority = std::pair<TSystemTimestamp, ui64>;
    std::vector<std::pair<TMessagesPriorityQueue*, std::function<TPriority()>>> queues;
    size_t queuedMessageCount = 0;
    for (auto& [streamId, streamState] : StreamStates_) {
        if (!streamState.Messages.empty() && allowedStreams.contains(streamId)) {
            auto bias = ComputeStreamBias(streamId, streamState.Messages.front().Message);
            queues.emplace_back(&streamState.Messages, [bias, messagesPtr = &streamState.Messages] () -> TPriority {
                YT_ASSERT(!messagesPtr->empty());
                const auto& front = messagesPtr->front();
                return {TSystemTimestamp(front.AlignmentTimestamp.Underlying() + bias), front.SeqNo};
            });
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
            batch.push_back(std::move(orderedMessage.Message));
        });

    if (!batchLimiter.IsFull()) {
        LastNotFullBatchInstant_ = TInstant::Now();
    }

    for (auto& [streamId, streamState] : StreamStates_) {
        streamState.LimitUsageState->Update(streamState.Usage);
    }

    return MakeFuture<std::vector<TInputMessageConstPtr>>(std::move(batch));
}

void TInputBuffer::RecalculateStreamLimits(TStreamState& streamState)
{
    using THeapElement = std::pair<TConnectionState*, i64>; // (it, currentBucketIndex).

    constexpr i64 lastEpochStoreCount = 5; // TODO: something better? Configurable?

    // Cleanup.
    for (auto it = streamState.ConnectionStates.begin(); it != streamState.ConnectionStates.end();) {
        if (it->second.UpdateEpoch + lastEpochStoreCount < streamState.Epoch) {
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

    streamState.Epoch += 1;
    streamState.RecalculateCounter = std::ssize(streamState.ConnectionStates);
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
