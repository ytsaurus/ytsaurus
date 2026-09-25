#pragma once

#include "public.h"

#include "kafka_client.h"

#include <yt/yt/flow/library/cpp/common/message.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/threading/atomic_object.h>
#include <library/cpp/yt/threading/spin_lock.h>

#include <contrib/libs/cppkafka/include/cppkafka/consumer.h>

#include <atomic>
#include <deque>
#include <memory>
#include <optional>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TKafkaMessage
{
    i64 Offset = 0;
    //! Backfilled write/event timestamps (unix seconds); never zero (poison-pill guard).
    TSystemTimestamp WriteTimestamp = ZeroSystemTimestamp;
    TSystemTimestamp CreateTimestamp = ZeroSystemTimestamp;
    //! Raw backfilled timestamp in unix seconds, surfaced as a source column.
    i64 TimestampSeconds = 0;
    //! Disengaged means a null Kafka buffer (null key / tombstone value), distinct from a present empty one.
    std::optional<std::string> Key;
    std::optional<std::string> Value;

    //! Bytes this message pins in the read buffer. The fixed overhead keeps empty records
    //! (tombstones) from bypassing the byte-based limits.
    i64 ByteSize() const
    {
        return static_cast<i64>(sizeof(TKafkaMessage)) +
            (Value ? std::ssize(*Value) : 0) +
            (Key ? std::ssize(*Key) : 0);
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Poll-loop backpressure: pause at the cap, resume once drained to half. The resume comparison is
//! inclusive so that max_buffer_bytes = 1 (whose half rounds to zero) can still resume.
bool ShouldPauseKafkaFetch(i64 bufferBytes, i64 maxBufferBytes);
bool ShouldResumeKafkaFetch(i64 bufferBytes, i64 maxBufferBytes);

//! librdkafka pre-fetch queue cap: half of |maxBufferBytes| in KiB, clamped into librdkafka's
//! accepted range (a larger value makes configuration fail).
i64 DeriveKafkaQueuedMaxKbytes(i64 maxBufferBytes);

////////////////////////////////////////////////////////////////////////////////

//! The buffer shared by the read session's poll thread and the source fiber; owns all their shared
//! state, so the cursor arithmetic is testable without a broker. Born unstarted: the poll thread
//! resolves the start offset (Flow's persisted offset, or the consumer group's when that is honored
//! and ahead) and calls #Start once; until then nothing is assigned, buffered or served.
class TKafkaReadBuffer
{
public:
    TKafkaReadBuffer() = default;

    bool IsStarted() const;

    //! Sets the start offset and requests the initial assign.
    void Start(i64 initialOffset);

    //! Source side. Returns buffered messages with Offset in [nextOffset, offsetLimitExclusive), up
    //! to the given limits. If |nextOffset| is not the continuation of what has been served, requests
    //! a seek instead and returns empty.
    std::vector<TKafkaMessage> GetBatch(
        i64 nextOffset,
        std::optional<i64> offsetLimitExclusive,
        i64 maxRows,
        i64 maxBytes);

    //! Poll side. The pending seek target; nullopt while unstarted. Stays pending until
    //! #ConfirmReassign, so a failed assign is retried.
    std::optional<i64> PeekReassign() const;

    //! Poll side. Marks the seek to |offset| applied; a no-op if a newer seek arrived in the meantime.
    void ConfirmReassign(i64 offset);

    //! Poll side. Appends polled messages, dropping anything below the fetch cursor.
    void Push(std::vector<TKafkaMessage> messages);

    i64 GetFetchCursor() const;

    i64 GetBufferBytes() const;

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    std::deque<TKafkaMessage> Buffer_;
    i64 BufferBytes_ = 0;
    bool Started_ = false;
    //! Poll loop's buffering position: the next offset it expects to buffer. Polled messages below it
    //! are dropped. Reset to the seek target when a re-assign is applied.
    i64 FetchCursor_ = 0;
    //! The offset the base is expected to request next (last served offset + 1); GetBatch treats a
    //! mismatch as a seek.
    i64 ExpectedNextOffset_ = 0;
    bool ReassignPending_ = false;
    i64 ReassignOffset_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! One sample of a partition's broker-side offsets, taken by the poll thread.
struct TKafkaWatermarks
{
    //! Log-start offset: the oldest offset retention still keeps.
    i64 Low = 0;
    //! High watermark: the first offset no consumer can read yet.
    i64 High = 0;
    //! The highest offset Flow had fetched or persisted when #High was queried.
    i64 ConsumedOffsetExclusiveAtQuery = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Owns one cppkafka::Consumer, manually assigned to one topic partition, and a poll loop on a
//! dedicated thread that fills the bounded buffer. Flow owns the offsets; the Kafka group commit is
//! best-effort lag telemetry and, unless |useConsumerGroupOffset| is off, the start position an
//! operator can move. All consumer calls happen on the poll thread.
class TKafkaReadSession
    : public TRefCounted
{
public:
    using TKafkaMessage = ::NYT::NFlow::TKafkaMessage;

    TKafkaReadSession(
        TKafkaClientPtr client,
        std::string topic,
        int partitionIndex,
        std::string groupId,
        i64 persistedOffsetExclusive,
        bool useConsumerGroupOffset,
        i64 maxBufferBytes,
        TDuration pollTimeout,
        TDuration watermarkUpdatePeriod,
        TDuration metadataTimeout,
        NLogging::TLogger logger,
        IStatusErrorStatePtr readErrorState);

    void Start();
    //! Must be called before the object is destroyed.
    void Terminate();

    void Reconfigure(TDuration pollTimeout, TDuration watermarkUpdatePeriod);

    //! Non-blocking: returns what the poll loop has buffered for [nextOffset, offsetLimitExclusive);
    //! an unexpected |nextOffset| schedules a seek and returns empty.
    std::vector<TKafkaMessage> GetBatch(
        i64 nextOffset,
        std::optional<i64> offsetLimitExclusive,
        i64 maxRows,
        i64 maxBytes);

    //! Best-effort commit of the persisted offset to the Kafka consumer group (lag telemetry only).
    void ReportPersistedOffset(i64 offsetExclusive);

    //! Latest watermark sample, or nullopt until the first successful query.
    std::optional<TKafkaWatermarks> GetWatermarks() const;

    //! The offset the poll loop started at: the persisted offset, or the group's committed offset when
    //! that is honored and ahead. Nullopt until resolved; never moves afterwards.
    std::optional<i64> GetStartOffset() const;

private:
    void PollLoop();
    TKafkaMessage ExtractMessage(const cppkafka::Message& message) const;

    const NLogging::TLogger Logger;
    const TKafkaClientPtr Client_;
    const std::string Topic_;
    const int PartitionIndex_;
    const std::string GroupId_;
    const i64 MaxBufferBytes_;
    //! Budget for blocking broker RPCs (metadata, query_offsets); polling passes its own timeout.
    const TDuration MetadataTimeout_;
    const IStatusErrorStatePtr ReadErrorState_;
    const bool UseConsumerGroupOffset_;

    std::atomic<i64> PollTimeoutMs_;
    std::atomic<i64> WatermarkUpdatePeriodMs_;

    NConcurrency::TActionQueuePtr PollQueue_;
    std::atomic<bool> Terminated_ = false;

    //! Latest offset the source has persisted; the poll thread commits it and stamps the watermark
    //! sample with it.
    std::atomic<i64> PersistedOffsetExclusive_;

    //! Published by the poll thread.
    NThreading::TAtomicObject<std::optional<TKafkaWatermarks>> Watermarks_;

    //! Published by the poll thread; -1 until resolved.
    std::atomic<i64> StartOffset_ = -1;

    TKafkaReadBuffer Buffer_;
};

DEFINE_REFCOUNTED_TYPE(TKafkaReadSession);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
