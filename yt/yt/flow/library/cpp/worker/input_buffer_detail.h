#pragma once

#include "input_buffer.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/message_batcher.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <library/cpp/containers/absl/flat_hash_map.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

#include <util/random/shuffle.h>

#include <limits>

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

class TInputBuffer
    : public IInputBuffer
{
public:
    struct TOrderedMessage
    {
        TSystemTimestamp AlignmentTimestamp;
        ui64 SeqNo = 0;
        TInputMessageConstPtr Message;

        bool operator<(const TOrderedMessage& right) const;
    };

    // Min-heap ordered by (AlignmentTimestamp, SeqNo). Keys are inline, so comparisons do not
    // dereference |Message|; pushing a run of messages in key order costs O(1) per element.
    class TMessagesPriorityQueue
    {
    public:
        bool empty() const;
        size_t size() const;
        const TOrderedMessage& front() const;
        void push(TOrderedMessage&& message);
        TOrderedMessage extract_front();

    private:
        std::vector<TOrderedMessage> Heap_;
        size_t HighWatermark_ = 0;
        size_t ExtractionsSinceShrinkCheck_ = 0;
    };

    struct TMessageState
    {
        TStreamId StreamId;
        i64 ByteSize = 0;
        EMessageDeliveryState CurrentDeliveryState;
        TCompactVector<TOnProcessedCallback, 1> Subscribers;
        TInstant RegisterTime;
    };

    struct TConnectionState
    {
        i64 UpdateEpoch{};
        TConnectionStreamOffer Offer;
        bool FreshOffer = false; // First bucket of offer can be accepted even if it is larger than available free space.
        i64 InflatedByteLimit = 0;

        void Acquire(i64 inflatedSize);
    };

    struct TStreamState
    {
        using TConnectionStates = THashMap<TGuid, TConnectionState>;

        TConnectionStates ConnectionStates;
        i64 RecalculateCounter = 0;
        i64 Epoch = 0; // How many recalculations (~ updates of all connections) were done.

        TMessagesPriorityQueue Messages;

        NProfiling::TCounter PersistedMessagesCounter;
        NProfiling::TCounter PersistedBytesCounter;

        i64 NotPersistedMessageCount = 0;
        NProfiling::TGauge NotPersistedMessageGauge;

        TStreamUsage Usage;

        NFlow::TStreamLimitUsageStatePtr LimitUsageState;
    };

public:
    // TODO(gryzlov-ad): Make configuration more granular.
    TInputBuffer(
        TJobId jobId,
        NFlow::TStreamLimitUsageStateMap streamLimitUsageStates,
        TComputationSpecPtr computationSpec,
        TComputationId computationId,
        TDynamicComputationSpecPtr dynamicSpec,
        IInvokerPtr finalizerPoolInvoker,
        NProfiling::TProfiler profiler);

    ~TInputBuffer() noexcept override;

    void Reconfigure(TDynamicComputationSpecPtr dynamicSpec) override;
    void UpdateMessageTransferingInfo(TMessageTransferingInfoPtr messageTransferingInfo) override;

    TFuture<std::vector<EMessageDeliveryState>> AddMessages(
        TGuid connectionId,
        std::vector<TInputMessageConstPtr> messages,
        TOnProcessedCallback onProcessed) override;
    void AddConnectionOffer(TGuid connectionId, TConnectionOffer offer) override;
    TFuture<THashMap<TStreamId, i64>> GetConnectionLimits(TGuid connectionId) override;
    void MarkPersisted(std::deque<TMessageId> messageIds) override;

    TFuture<std::vector<TInputMessageConstPtr>> GetInputBatch(const THashSet<TStreamId>& allowedStreams) override;

    TSystemTimestamp GetMinStabilizedEventTimestamp() override;

    static void RecalculateStreamLimits(TStreamState& streamState);
    static i64 GetPendingSize(const TStreamState& streamState);

    double ComputeStreamBias(TStreamId streamId, const TInputMessageConstPtr& frontMessage) const;

    const TComputationId& GetComputationId() override;

private:
    const TJobId JobId_;
    const TInputOrderingSpecPtr OrderingSpec_;
    const TComputationId ComputationId_;
    const IInvokerPtr FinalizerPoolInvoker_;
    const IInvokerPtr SerializedInvoker_;

    absl::flat_hash_map<TMessageId, TMessageState> MessageStatesMap_;
    THashMap<TStreamId, TStreamState> StreamStates_;
    ui64 NextSeqNo_ = 0;

    TMessageBatchLimiter BatchLimiter_;
    TDuration BatchDuration_;
    TInstant LastNotFullBatchInstant_;

    TMessageTransferingInfoPtr MessageTransferingInfo_;

    NProfiling::TEventTimer MessageProcessingTimer_;

    void DoReconfigure(TDynamicComputationSpecPtr dynamicSpec);
    void DoUpdateMessageTransferingInfo(TMessageTransferingInfoPtr messageTransferingInfo);
    std::vector<EMessageDeliveryState> DoAddMessages(
        TGuid connectionId,
        std::vector<TInputMessageConstPtr> messages,
        TOnProcessedCallback onProcessed,
        TInstant now);
    void DoAddConnectionOffer(TGuid connectionId, TConnectionOffer offer);
    THashMap<TStreamId, i64> DoGetConnectionLimits(TGuid connectionId);
    void DoMarkPersisted(std::deque<TMessageId> messageIds, TInstant now);
    TFuture<std::vector<TInputMessageConstPtr>> DoGetInputBatch(THashSet<TStreamId> allowedStreams);
    TSystemTimestamp DoGetMinStabilizedEventTimestamp();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
