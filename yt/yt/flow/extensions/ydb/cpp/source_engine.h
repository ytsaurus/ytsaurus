#pragma once

#include "public.h"

#include "options.h"

#include <yt/yt/flow/library/cpp/connectors/common/ordered_source_base.h>

#include <yt/yt/flow/library/cpp/common/message_batcher.h>
#include <yt/yt/flow/library/cpp/common/source.h>

#include <yt/yt/flow/library/cpp/misc/public.h>

#include <yt/yt/core/actions/signal.h>
#include <yt/yt/core/concurrency/public.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>
#include <library/cpp/yt/memory/ref.h>

#include <ydb-cpp-sdk/client/topic/client.h>

#include <deque>
#include <set>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

NTableClient::TTableSchemaPtr GetYdbTopicSourceSchema(
    const std::optional<std::string>& attributesColumn,
    const std::optional<std::string>& frameMetaColumn = std::nullopt,
    const std::optional<std::string>& messageMetadataColumn = std::nullopt,
    const std::optional<std::string>& writeTimeColumn = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

struct TYdbPartitionInfoUpdate
{
    std::optional<i64> CommittedOffsetExclusive;
    std::optional<i64> MaxOffsetExclusive;
    std::optional<TInstant> UpdateInstant;
    bool Repositioned = false;
};

using TYdbPartitionInfoUpdater = TCallback<void(const TYdbPartitionInfoUpdate&)>;

////////////////////////////////////////////////////////////////////////////////

class TYdbSourceReadSession final
{
public:
    using TThis = TYdbSourceReadSession;

    struct TCompressedMessage
    {
        i64 Offset;
        TSystemTimestamp WriteTimestamp;
        TSystemTimestamp CreateTimestamp;
        ui64 WriteTimeMilliseconds = 0;
        std::optional<TFlowQueueMeta> Meta;
        TYdbTopicAttributes Attributes;
        NYson::TYsonString MessageMetadata = {};
        std::shared_ptr<const std::string> Data;
        NYdb::NTopic::ECodec Codec;
    };

    using TOffsetsHandleSignature = void(i64, std::optional<i64>, std::optional<TInstant>);
    using TPartitionEndHandleSignature = void(i64);

    TYdbSourceReadSession(
        TSourceContextPtr context,
        std::shared_ptr<const TYdbSourceOptions> options,
        std::shared_ptr<const TYdbSourceDynamicOptions> dynamicOptions,
        std::shared_ptr<NYdb::NTopic::TTopicClient> topicClient,
        std::string topic,
        i64 partitionIndex,
        i64 persistedOffsetExclusive,
        NLogging::TLogger logger,
        IStatusErrorStatePtr readErrorState);

    DEFINE_SIGNAL(TOffsetsHandleSignature, OffsetsUpdate);
    //! Fired with the offset past the last received message once the topic reports that the partition gets
    //! no more writes. Retention may have moved the actual end past it.
    DEFINE_SIGNAL(TPartitionEndHandleSignature, PartitionEnd);

    void Init();
    void Reconfigure(std::shared_ptr<const TYdbSourceDynamicOptions> dynamicOptions);
    void SetPartitionEndOffset(i64 endOffsetExclusive);
    // Must be called before destroy.
    void Terminate();

    // Returns: (canContinueReading, message).
    std::pair<bool, std::optional<TCompressedMessage>> GetNextMessage(i64 nextOffset, std::optional<i64> offsetLimitExclusive);
    // Completes when the current session may have a new event or when |timeout| expires.
    TFuture<void> WaitForEvent(TDuration timeout);
    void ReportPersistedOffset(i64 offset);

    void RequestStatus();
    std::optional<std::string> GetReadSessionId() const;

    //! Whether a session is established right now; while it is not, #Execute is inside its own connect
    //! loop, bounded by |StartSessionTimeout| and reporting its own errors.
    bool IsSessionEstablished() const;

    //! Whether the topic has been heard from since |instant| on the session running right now: a message,
    //! a status answer or a commit ack. Establishing a session is deliberately not a signal — a wedged
    //! partition still lets us connect — and neither is a signal delivered by a session that has since
    //! died, which says nothing about the one that replaced it.
    bool HasSignalSince(TInstant instant) const;

    //! For how long the topic has said nothing, counted from the later of the last signal and the
    //! establishment of the current session, so that a fresh session gets a full budget before it is judged.
    TDuration GetSilenceDuration() const;

private:
    const NLogging::TLogger Logger;

    const TSourceContextPtr Context_;
    const std::shared_ptr<const TYdbSourceOptions> Options_;
    std::shared_ptr<const TYdbSourceDynamicOptions> DynamicOptions_;
    IStatusErrorStatePtr ReadErrorState_;

    const std::shared_ptr<NYdb::NTopic::TTopicClient> TopicClient_;
    const std::string Topic_;
    const i64 PartitionIndex_;

    i64 PersistedOffsetExclusive_;
    TInstant LastSignalInstant_;
    TInstant SessionStartInstant_;

    TFuture<void> Executor_;

    struct TSession
    {
        std::shared_ptr<NYdb::NTopic::IReadSession> Read = nullptr;
        NYdb::NTopic::TPartitionSession::TPtr Partition = nullptr;
        // Offset past the last message the topic had when the session started.
        i64 StartEndOffset = 0;
        TFuture<void> EventFuture;
        std::deque<TCompressedMessage> Buffered = {};
        bool HasRead = false;
        bool Ended = false;
        std::optional<NYdb::NTopic::TReadSessionEvent::TEndPartitionSessionEvent> UnconfirmedEnd;
        i64 ReceivedOffsetExclusive = 0;
        i64 CommittedOffsetExclusive = 0;
        std::set<i64> InflightOffsets_;
    };

    std::shared_ptr<TSession> Session_;
    std::optional<i64> PartitionEndOffsetExclusive_;

private:
    void Execute();
    std::shared_ptr<TSession> InitSession(TPromise<void> errorPromise);
    void TryConfirmPartitionEnd();
    void OnCommittedOffset(i64 committedOffsetExclusive);
    void OnPartitionSessionStatus(
        const NYdb::NTopic::TReadSessionEvent::TPartitionSessionStatusEvent& event);
    void OnPartitionSessionClosed(
        TPromise<void> errorPromise,
        const NYdb::NTopic::TReadSessionEvent::TPartitionSessionClosedEvent& event);
    void OnSessionClosed(
        TPromise<void> errorPromise,
        const NYdb::NTopic::TSessionClosedEvent& event);
    void OnCommitOffsetAcknowledgement(
        const NYdb::NTopic::TReadSessionEvent::TCommitOffsetAcknowledgementEvent& event);
    void OnStopPartitionSession(
        TPromise<void> errorPromise,
        const NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent& event);
};

DEFINE_REFCOUNTED_TYPE(TYdbSourceReadSession);

////////////////////////////////////////////////////////////////////////////////

//! Reads one topic partition; owned by a schema-bound source and driven from its serialized invoker.
class TYdbSourceEngine
    : public TRefCounted
{
public:
    TYdbSourceEngine(
        TSourceContextPtr context,
        std::shared_ptr<const TYdbSourceOptions> options,
        std::shared_ptr<const TYdbSourceDynamicOptions> dynamicOptions,
        IYdbClientFactoryPtr clientFactory,
        const NLogging::TLogger& logger,
        const NProfiling::TProfiler& profiler,
        TYdbPartitionInfoUpdater updatePartitionInfo);

    void Init(IStatusErrorStatePtr readErrorState, IStatusErrorStatePtr sessionLivenessErrorState);
    void Terminate();
    void ReportPersistedOffset(i64 offset);
    void Reconfigure(std::shared_ptr<const TYdbSourceDynamicOptions> dynamicOptions);
    //! A blocked engine keeps no read session and returns no records.
    void SetReadBlocked(bool readBlocked);
    std::optional<i64> GetEndOffsetExclusive() const;
    bool IsCommitBehindPartitionEnd() const;

    TFuture<std::vector<TOrderedSourceBase::TRecord>> ReadNextBatch(
        const TMessageBatcherSettingsPtr& settings,
        i64 nextOffset,
        std::optional<i64> offsetLimitExclusive);

    //! Reads toward |endExclusive| regardless of the batch row and byte limits of |settings|.
    TFuture<std::vector<TOrderedSourceBase::TRecord>> ReadRange(
        const TMessageBatcherSettingsPtr& settings,
        i64 nextOffset,
        i64 endExclusive);

    TFuture<std::optional<i64>> FetchTrimmedOffset();

    void SetAppliedMalformedMessagePolicy(EMalformedYdbMessagePolicy policy);
    EMalformedYdbMessagePolicy GetMalformedMessagePolicy() const;

    std::pair<std::vector<TPayload>, NTableClient::TTableSchemaPtr> ProcessMessage(const TYdbSourceReadSession::TCompressedMessage& message);

private:
    const TSourceContextPtr Context_;
    const std::shared_ptr<const TYdbSourceOptions> Options_;
    std::shared_ptr<const TYdbSourceDynamicOptions> DynamicOptions_;
    const TYdbPartitionInfoUpdater UpdatePartitionInfo_;
    const NTableClient::TTableSchemaPtr Schema_;
    const std::string Database_;
    const std::string Endpoint_;
    const std::string Topic_;
    const i64 PartitionIndex_;

    const NLogging::TLogger Logger;

    const std::shared_ptr<NYdb::NTopic::TTopicClient> TopicClient_;

    NConcurrency::TPeriodicExecutorPtr PartitionInfoUpdater_;

    TYdbSourceReadSessionPtr ReadSession_;
    bool Initialized_ = false;
    bool ReadBlocked_ = false;
    std::optional<i64> ReceivedEndOffsetExclusive_;
    i64 CommittedOffsetExclusive_ = 0;
    //! Set once the end of an inactive partition is confirmed by its description.
    std::optional<i64> EndOffsetExclusive_;

    IStatusErrorStatePtr UpdatePartitionInfoErrorState_;
    IStatusErrorStatePtr ReadErrorState_;
    // Separate from the read error state on purpose: a wedged session is not a read error, and read
    // errors are what one looks for in the UI.
    IStatusErrorStatePtr SessionLivenessErrorState_;

    NProfiling::TCounter MalformedMessagesCounter_;
    std::optional<EMalformedYdbMessagePolicy> AppliedMalformedMessagePolicy_;

    i64 PersistedOffsetExclusive_ = 0;
    TInstant PersistedOffsetChangeInstant_ = TInstant::Zero(); // Zero at start and if it is equal to committed.
    TInstant LastLivenessCheckInstant_ = TInstant::Zero();
    // Returning a source batch starts its computation work; the next non-full batch is paced from
    // this instant so processing and commit time consume the batch-duration budget.
    std::optional<TInstant> LastBatchStartInstant_;

private:
    TFuture<std::vector<TOrderedSourceBase::TRecord>> ReadRecords(
        const TMessageBatcherSettingsPtr& settings,
        i64 nextOffset,
        std::optional<i64> offsetLimitExclusive,
        i64 maxRowsPerBatch,
        i64 maxBytesPerBatch);

    void StartReadSession();
    void StopReadSession();

    void UpdatePartitionOffsets(
        i64 committedOffsetExclusive,
        std::optional<i64> maxOffsetExclusive = std::nullopt,
        std::optional<TInstant> instant = std::nullopt);
    void OnPartitionEnd(i64 receivedEndOffsetExclusive);
    void ConfirmPartitionEnd();

    void DoHardCommit(i64 offset, const std::string& readSessionId);
    void TryUpdatePartitionInfo();
    void EnsureReadSessionLiveness();
};

DEFINE_REFCOUNTED_TYPE(TYdbSourceEngine);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
