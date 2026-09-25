#include "read_session.h"

#include "private.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <contrib/libs/cppkafka/include/cppkafka/buffer.h>
#include <contrib/libs/cppkafka/include/cppkafka/error.h>
#include <contrib/libs/cppkafka/include/cppkafka/message.h>
#include <contrib/libs/cppkafka/include/cppkafka/topic_partition.h>
#include <contrib/libs/cppkafka/include/cppkafka/topic_partition_list.h>

#include <util/datetime/base.h>

#include <algorithm>
#include <chrono>
#include <limits>

namespace NYT::NFlow {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr int PollBatchSize = 1000;

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool ShouldPauseKafkaFetch(i64 bufferBytes, i64 maxBufferBytes)
{
    return bufferBytes >= maxBufferBytes;
}

bool ShouldResumeKafkaFetch(i64 bufferBytes, i64 maxBufferBytes)
{
    return bufferBytes <= maxBufferBytes / 2;
}

i64 DeriveKafkaQueuedMaxKbytes(i64 maxBufferBytes)
{
    constexpr i64 MaxAcceptedKbytes = std::numeric_limits<int>::max() / 1024;
    return std::clamp<i64>(maxBufferBytes / 2 / 1024, 1, MaxAcceptedKbytes);
}

////////////////////////////////////////////////////////////////////////////////

bool TKafkaReadBuffer::IsStarted() const
{
    auto guard = Guard(Lock_);

    return Started_;
}

void TKafkaReadBuffer::Start(i64 initialOffset)
{
    auto guard = Guard(Lock_);

    YT_VERIFY(!Started_);
    Started_ = true;
    FetchCursor_ = initialOffset;
    ExpectedNextOffset_ = initialOffset;
    ReassignPending_ = true;
    ReassignOffset_ = initialOffset;
}

std::vector<TKafkaMessage> TKafkaReadBuffer::GetBatch(
    i64 nextOffset,
    std::optional<i64> offsetLimitExclusive,
    i64 maxRows,
    i64 maxBytes)
{
    auto guard = Guard(Lock_);

    // Unstarted: nothing has been fetched, and treating |nextOffset| as a seek would defeat the resolution.
    if (!Started_) {
        return {};
    }

    if (nextOffset != ExpectedNextOffset_) {
        // Not the continuation of what was served (trim rewind, or the first read): a seek. The poll
        // loop applies it via re-assign.
        ReassignPending_ = true;
        ReassignOffset_ = nextOffset;
        FetchCursor_ = nextOffset;
        ExpectedNextOffset_ = nextOffset;
        Buffer_.clear();
        BufferBytes_ = 0;
        return {};
    }

    std::vector<TKafkaMessage> result;
    i64 bytes = 0;
    while (!Buffer_.empty() && std::ssize(result) < maxRows && bytes < maxBytes) {
        auto& front = Buffer_.front();
        if (offsetLimitExclusive && front.Offset >= *offsetLimitExclusive) {
            break;
        }
        // Defensive: never surface an offset below what the base asked for (would trip its YT_VERIFY).
        if (front.Offset < nextOffset) {
            BufferBytes_ -= front.ByteSize();
            Buffer_.pop_front();
            continue;
        }
        bytes += front.ByteSize();
        BufferBytes_ -= front.ByteSize();
        ExpectedNextOffset_ = front.Offset + 1;
        result.push_back(std::move(front));
        Buffer_.pop_front();
    }
    return result;
}

std::optional<i64> TKafkaReadBuffer::PeekReassign() const
{
    auto guard = Guard(Lock_);

    if (!ReassignPending_) {
        return std::nullopt;
    }
    return ReassignOffset_;
}

void TKafkaReadBuffer::ConfirmReassign(i64 offset)
{
    auto guard = Guard(Lock_);

    // A newer seek may have been requested while assign was in flight; keep it pending so the next
    // iteration re-assigns to the newer target instead of settling on this stale one.
    if (!ReassignPending_ || ReassignOffset_ != offset) {
        return;
    }
    ReassignPending_ = false;
    FetchCursor_ = ReassignOffset_;
    Buffer_.clear();
    BufferBytes_ = 0;
}

void TKafkaReadBuffer::Push(std::vector<TKafkaMessage> messages)
{
    auto guard = Guard(Lock_);

    // Messages fetched before a pending seek are stale; buffering them would let GetBatch silently
    // skip the range the base asked to re-read. An unstarted buffer has no position at all.
    if (!Started_ || ReassignPending_) {
        return;
    }

    for (auto& message : messages) {
        if (message.Offset >= FetchCursor_) {
            BufferBytes_ += message.ByteSize();
            FetchCursor_ = message.Offset + 1;
            Buffer_.push_back(std::move(message));
        }
    }
}

i64 TKafkaReadBuffer::GetFetchCursor() const
{
    auto guard = Guard(Lock_);
    return FetchCursor_;
}

i64 TKafkaReadBuffer::GetBufferBytes() const
{
    auto guard = Guard(Lock_);
    return BufferBytes_;
}

////////////////////////////////////////////////////////////////////////////////

TKafkaReadSession::TKafkaReadSession(
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
    IStatusErrorStatePtr readErrorState)
    : Logger(std::move(logger))
    , Client_(std::move(client))
    , Topic_(std::move(topic))
    , PartitionIndex_(partitionIndex)
    , GroupId_(std::move(groupId))
    , MaxBufferBytes_(maxBufferBytes)
    , MetadataTimeout_(metadataTimeout)
    , ReadErrorState_(std::move(readErrorState))
    , UseConsumerGroupOffset_(useConsumerGroupOffset)
    , PollTimeoutMs_(pollTimeout.MilliSeconds())
    , WatermarkUpdatePeriodMs_(watermarkUpdatePeriod.MilliSeconds())
    , PersistedOffsetExclusive_(persistedOffsetExclusive)
{ }

void TKafkaReadSession::Start()
{
    PollQueue_ = New<TActionQueue>("KafkaPoll");
    PollQueue_->GetInvoker()->Invoke(BIND(&TKafkaReadSession::PollLoop, MakeWeak(this)));
}

void TKafkaReadSession::Terminate()
{
    Terminated_.store(true);
    if (PollQueue_) {
        // Shutdown joins the poll thread, which may be inside a blocking librdkafka call. Run the join
        // on a background invoker and WaitFor it so the (serialized-invoker) caller fiber yields instead
        // of pinning its OS thread.
        auto queue = std::move(PollQueue_);
        WaitFor(BIND([queue = std::move(queue)] {
            queue->Shutdown(/*graceful*/ true);
        })
                .AsyncVia(GetFinalizerInvoker())
                .Run())
            .ThrowOnError();
    }
}

void TKafkaReadSession::Reconfigure(TDuration pollTimeout, TDuration watermarkUpdatePeriod)
{
    PollTimeoutMs_.store(pollTimeout.MilliSeconds());
    WatermarkUpdatePeriodMs_.store(watermarkUpdatePeriod.MilliSeconds());
}

std::vector<TKafkaMessage> TKafkaReadSession::GetBatch(
    i64 nextOffset,
    std::optional<i64> offsetLimitExclusive,
    i64 maxRows,
    i64 maxBytes)
{
    return Buffer_.GetBatch(nextOffset, offsetLimitExclusive, maxRows, maxBytes);
}

void TKafkaReadSession::ReportPersistedOffset(i64 offsetExclusive)
{
    PersistedOffsetExclusive_.store(offsetExclusive);
}

std::optional<TKafkaWatermarks> TKafkaReadSession::GetWatermarks() const
{
    return Watermarks_.Load();
}

std::optional<i64> TKafkaReadSession::GetStartOffset() const
{
    auto startOffset = StartOffset_.load();
    if (startOffset < 0) {
        return std::nullopt;
    }
    return startOffset;
}

TKafkaMessage TKafkaReadSession::ExtractMessage(const cppkafka::Message& message) const
{
    TKafkaMessage result;
    result.Offset = message.get_offset();

    // Presence is the pointer, not the size: a tombstone has a null value and an empty-keyed record
    // has a present zero-length key; collapsing either would corrupt compaction and routing semantics.
    const auto& payload = message.get_payload();
    if (payload.get_data() != nullptr) {
        result.Value.emplace(reinterpret_cast<const char*>(payload.get_data()), payload.get_size());
    }

    const auto& key = message.get_key();
    if (key.get_data() != nullptr) {
        result.Key.emplace(reinterpret_cast<const char*>(key.get_data()), key.get_size());
    }

    // Backfill: prefer the Kafka message timestamp (CreateTime or broker LogAppendTime); if it is
    // absent or non-positive, fall back to the read instant. Never leave it zero, or PrepareMessages
    // throws and wedges the partition.
    i64 seconds = 0;
    if (auto timestamp = message.get_timestamp()) {
        seconds = std::chrono::duration_cast<std::chrono::seconds>(timestamp->get_timestamp()).count();
    }
    if (seconds <= 0) {
        seconds = TInstant::Now().Seconds();
    }
    result.TimestampSeconds = seconds;
    result.WriteTimestamp = TSystemTimestamp(seconds);
    result.CreateTimestamp = TSystemTimestamp(seconds);

    return result;
}

void TKafkaReadSession::PollLoop()
{
    std::unique_ptr<cppkafka::Consumer> consumer;
    try {
        auto configuration = Client_->MakeBaseConfiguration();
        configuration.set("group.id", GroupId_);
        configuration.set("enable.auto.commit", "false");
        // Flow assigns explicit offsets, so out-of-range is always a data discontinuity; "earliest"
        // would silently restart a recreated topic from zero beneath the stale cursor. A retention
        // trim recovers explicitly: the watermark update reports it and the base rewinds.
        configuration.set("auto.offset.reset", "error");
        // Cap librdkafka's own pre-fetch queue (64 MiB default) at half our budget:
        // ~1.5 max_buffer_bytes total per partition.
        configuration.set("queued.max.messages.kbytes", ToString(DeriveKafkaQueuedMaxKbytes(MaxBufferBytes_)));
        consumer = std::make_unique<cppkafka::Consumer>(std::move(configuration));
        // Budget for blocking broker RPCs (query_offsets and friends); the poll timeout would starve
        // them, and polling passes its own timeout per call anyway.
        consumer->set_timeout(std::chrono::milliseconds(MetadataTimeout_.MilliSeconds()));
    } catch (const std::exception& ex) {
        auto error = TError("Failed to create Kafka consumer").With(ex);
        YT_TLOG_ERROR("Failed to create Kafka consumer")
            .With(ex);
        ReadErrorState_->SetError(error);
        return;
    }

    const cppkafka::TopicPartition partition(Topic_, PartitionIndex_);
    bool paused = false;
    auto lastWatermarkQuery = TInstant::Zero();
    // The offset the session was started with is already on the broker side; commit only what the
    // source persists from here on.
    i64 lastCommittedOffset = PersistedOffsetExclusive_.load();

    while (!Terminated_.load()) {
        try {
            // Apply a pending (re)assign; this is also the initial assign. Confirmed only after
            // assign succeeds, so a throw leaves the seek pending and retried.
            if (auto reassignOffset = Buffer_.PeekReassign()) {
                consumer->assign({cppkafka::TopicPartition(Topic_, PartitionIndex_, *reassignOffset)});
                // librdkafka keeps the application-pause flag across assign; without an explicit
                // resume, a seek issued while paused would stay paused forever.
                consumer->resume();
                paused = false;
                Buffer_.ConfirmReassign(*reassignOffset);
            }

            // Best-effort commit of the persisted offset to the consumer group (lag telemetry only). Never
            // below the start offset: until Flow adopts it, its checkpoint lags the group, and committing
            // that would rewind a reset the operator has just made. Held back until the start is known.
            if (Buffer_.IsStarted()) {
                auto commitOffset = std::max(PersistedOffsetExclusive_.load(), StartOffset_.load());
                if (commitOffset != lastCommittedOffset) {
                    lastCommittedOffset = commitOffset;
                    try {
                        consumer->async_commit(cppkafka::TopicPartitionList{
                            cppkafka::TopicPartition(Topic_, PartitionIndex_, commitOffset)});
                    } catch (const std::exception& ex) {
                        // Committing below the log-start offset after retention is expected; ignore.
                        YT_TLOG_DEBUG("Best-effort Kafka offset commit failed")
                            .With("Offset", commitOffset)
                            .With(ex);
                    }
                }
            }

            // Refresh watermarks periodically, stamping the attempt: a fast-failing query must retry
            // once per period, not every iteration.
            auto now = TInstant::Now();
            if (now - lastWatermarkQuery >= TDuration::MilliSeconds(WatermarkUpdatePeriodMs_.load())) {
                lastWatermarkQuery = now;

                // The group's committed offset is how an operator repositions a partition: honored when
                // ahead of Flow's checkpoint, ignored when behind. Read before the watermarks: on an
                // intact topic a committed offset never exceeds a high watermark sampled later, so a
                // larger one proves the topic lost records. Consulted before the first assign, so
                // nothing below it is fetched.
                std::optional<i64> groupOffset;
                if (!Buffer_.IsStarted() && UseConsumerGroupOffset_) {
                    auto committed = consumer->get_offsets_committed({partition});
                    YT_VERIFY(committed.size() == 1);
                    if (committed[0].get_offset() >= 0) {
                        groupOffset = committed[0].get_offset();
                    }
                }

                auto [low, high] = consumer->query_offsets(partition);
                // A protocol-compatible broker may answer a not-yet-initialised partition with -1 and
                // no error; librdkafka forwards it. Treat it as a failed query rather than publish a
                // sample that reads as a recreated topic.
                if (low < 0 || high < 0) {
                    THROW_ERROR_EXCEPTION("Kafka returned negative watermarks")
                        .With("low_watermark", low)
                        .With("high_watermark", high);
                }
                // Stamp the sample with everything Flow has taken from the topic so far, buffered or
                // persisted, read after the query returned. An intact topic's high watermark only
                // grows and every offset a consumer is served lies below it, so it covers all of that;
                // a lower one proves the partition lost records. Reading the stamp before the call, or
                // stamping the persisted offset alone, would let old-topic records still in the buffer
                // slip past a recreated topic's watermark.
                auto consumedOffsetExclusive = std::max(PersistedOffsetExclusive_.load(), Buffer_.GetFetchCursor());
                Watermarks_.Store(TKafkaWatermarks{
                    .Low = low,
                    .High = high,
                    .ConsumedOffsetExclusiveAtQuery = consumedOffsetExclusive,
                });

                if (!Buffer_.IsStarted()) {
                    i64 startOffset = PersistedOffsetExclusive_.load();
                    if (groupOffset && *groupOffset > startOffset) {
                        // Not clamped to the topic end: a clamp never reaches the group, so every
                        // restart would re-clamp to a grown watermark and skip the records appended
                        // in between. The partition stays unstarted, rechecking once per period,
                        // until the group is reset.
                        if (*groupOffset > high) {
                            THROW_ERROR_EXCEPTION("Kafka consumer group offset is beyond the topic end; reset the group offsets to restart the partition")
                                .With("group_offset", *groupOffset)
                                .With("high_watermark", high)
                                .With("persisted_offset_exclusive", startOffset);
                        }
                        startOffset = *groupOffset;
                    }
                    Buffer_.Start(startOffset);
                    StartOffset_.store(startOffset);
                    // Whichever side the start was taken from already holds it.
                    lastCommittedOffset = std::max(lastCommittedOffset, startOffset);
                    YT_TLOG_INFO("Kafka read session started")
                        .With("StartOffset", startOffset)
                        .With("PersistedOffsetExclusive", PersistedOffsetExclusive_.load())
                        .With("GroupOffset", groupOffset.value_or(-1))
                        .With("HighWatermark", high);
                }
            }

            // Backpressure: pause fetching while the buffer is full, resume once it drains.
            auto bufferBytes = Buffer_.GetBufferBytes();
            if (!paused && ShouldPauseKafkaFetch(bufferBytes, MaxBufferBytes_)) {
                consumer->pause();
                paused = true;
            } else if (paused && ShouldResumeKafkaFetch(bufferBytes, MaxBufferBytes_)) {
                consumer->resume();
                paused = false;
            }

            auto messages = consumer->poll_batch(
                PollBatchSize,
                std::chrono::milliseconds(PollTimeoutMs_.load()));

            // Extract without the lock, then append under one lock. An error event does not stop the
            // extraction: the messages behind it are already consumed from the fetcher's position,
            // and dropping them would silently skip their offsets.
            std::vector<TKafkaMessage> extracted;
            extracted.reserve(messages.size());
            TError pollError;
            for (const auto& message : messages) {
                if (!message) {
                    continue;
                }
                if (message.is_eof()) {
                    continue;
                }
                if (auto error = message.get_error()) {
                    if (pollError.IsOK()) {
                        pollError = TError("Kafka poll error: %v", error.to_string());
                    }
                    continue;
                }
                extracted.push_back(ExtractMessage(message));
            }

            if (!extracted.empty()) {
                Buffer_.Push(std::move(extracted));
            }

            pollError.ThrowOnError();

            // An unstarted session has nothing assigned, so its polls come back empty and prove nothing:
            // clearing on them would flip a start failure (group beyond the topic end, coordinator
            // down) to OK between retries and keep the base from ever declaring the partition
            // unavailable.
            if (Buffer_.IsStarted()) {
                ReadErrorState_->ClearError();
            }
        } catch (const std::exception& ex) {
            auto error = TError("Kafka read failed").With(ex);
            YT_TLOG_ERROR("Kafka read failed")
                .With(ex);
            ReadErrorState_->SetError(error);
            // An exception thrown before poll_batch (assign, commit, query_offsets) would otherwise
            // restart the loop with no wait at all, spinning the poll thread at full speed.
            Sleep(TDuration::MilliSeconds(PollTimeoutMs_.load()));
        }
    }

    try {
        consumer->unassign();
    } catch (const std::exception& ex) {
        YT_TLOG_DEBUG("Failed to unassign Kafka consumer on terminate")
            .With(ex);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
