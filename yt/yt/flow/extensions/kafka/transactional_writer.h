#pragma once

#include "public.h"

#include "sink.h"

#include <util/generic/size_literals.h>

#include <functional>
#include <span>

struct rd_kafka_consumer_group_metadata_s;
struct rd_kafka_error_s;

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! The progress marker of a #TTransactionalKafkaWriter is the committed offset of the consumer group named
//! after the transactional id: the offset is the seqNo of the last committed message, and the offset metadata
//! is this tag. An offset without it, e.g. after a reset, is not a marker.
constexpr TStringBuf KafkaProgressMarkerMetadata = "ytflow/v1";

//! A committed offset of the marker consumer group, as read back.
struct TKafkaCommittedOffset
{
    i64 Offset = 0;
    std::string Metadata;
};

//! What the persisted sink state says about the transactional writes of earlier sessions.
struct TKafkaTransactionalRecovery
{
    i64 MaxPersistedSeqNo = 0;
    //! See #TKafkaSinkState::MaxDistributedSeqNo.
    i64 MaxDistributedSeqNo = 0;
};

//! Decides up to which seqNo a restarted writer takes the messages as committed. |committed| is null when
//! the group has no offset. It is trusted only with #KafkaProgressMarkerMetadata and a seqNo within the range
//! the persisted state allows: a marker commits before its messages are acknowledged, so it is never behind
//! the persisted frontier, and never past the persisted bound. Returns null when nothing can be trusted; that
//! is an error if earlier sessions may have committed messages past the persisted frontier, as writing them
//! again may duplicate them.
TErrorOr<std::optional<i64>> ResolveKafkaProgressMarker(
    const std::optional<TKafkaCommittedOffset>& committed,
    const TKafkaTransactionalRecovery& recovery);

////////////////////////////////////////////////////////////////////////////////

struct TTransactionalKafkaWriterOptions
{
    std::string Topic;
    std::string TransactionalId;
    //! Header name for the Flow message id; empty disables the header.
    std::string MessageIdHeader;
    TDuration TransactionTimeout = TDuration::Minutes(1);
    i64 MaxTransactionRecordCount = 10'000;
    i64 MaxTransactionByteSize = 16_MB;
    //! See #TDynamicKafkaSinkParameters::AllowMissingProgressMarker.
    bool AllowMissingProgressMarker = false;
    //! Consecutive failed attempts of a broker request or a transaction after which the writer fails.
    int MaxConsecutiveFailures = 10;
    TDuration RetryBackoff = TDuration::Seconds(1);
};

//! Writes records in Kafka transactions under a transactional id that survives restarts; starting fences
//! the writers of earlier sessions. Each transaction also commits the seqNo of its last record as the progress
//! marker (see #KafkaProgressMarkerMetadata), and a restarted writer acknowledges the replayed messages the
//! marker covers without writing them. Promises resolve in seqNo order once their transaction commits. Any
//! error the writer cannot retry within its budget fails every write for good (see #GetFatalError()); a
//! restart then recovers from the marker.
class TTransactionalKafkaWriter
    : public TRefCounted
{
public:
    TTransactionalKafkaWriter(
        TKafkaClientPtr client,
        TTransactionalKafkaWriterOptions options,
        TKafkaTransactionalRecovery recovery,
        NLogging::TLogger logger,
        IStatusProfilerPtr statusProfiler);

    void Start();
    void Terminate();

    TFuture<void> Write(TKafkaMessageToWrite&& message);

    //! The error the writer failed with, or OK while it works.
    TError GetFatalError() const;

private:
    struct TCallResult;

    const TKafkaClientPtr Client_;
    const TTransactionalKafkaWriterOptions Options_;
    const TKafkaTransactionalRecovery Recovery_;
    const NLogging::TLogger Logger;
    const IStatusErrorStatePtr ErrorState_;

    NConcurrency::TActionQueuePtr WriteQueue_;
    std::atomic<bool> Terminated_ = false;

    TKafkaWriteQueue Queue_;

    void Run();
    void Fail(const TError& error);

    //! Fences earlier sessions and resolves the marker; returns an error if the writer cannot start.
    TErrorOr<std::optional<i64>> Initialize(cppkafka::Producer& producer);
    TErrorOr<std::optional<TKafkaCommittedOffset>> ReadCommittedOffset();

    //! Takes ownership of |error|, which is null on success.
    static TCallResult ClassifyTransactionError(rd_kafka_error_s* error, TStringBuf operation);
    //! Repeats |call| while it fails retriably, within the failure budget.
    TCallResult CallRetrying(const std::function<rd_kafka_error_s*()>& call, TStringBuf operation);
    //! Writes |records| and the marker for the last of them in one transaction, aborting it on an abortable
    //! error.
    TCallResult TryCommitTransaction(
        cppkafka::Producer& producer,
        const rd_kafka_consumer_group_metadata_s* groupMetadata,
        std::span<const TKafkaMessageToWrite> records);
};

DEFINE_REFCOUNTED_TYPE(TTransactionalKafkaWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
