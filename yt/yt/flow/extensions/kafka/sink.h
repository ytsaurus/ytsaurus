#pragma once

#include "public.h"

#include "kafka_client.h"
#include "spec.h"

#include <yt/yt/flow/library/cpp/connectors/common/ordered_async_sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sink_controller_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sync_sink_base.h>

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/public.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <deque>
#include <map>
#include <memory>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Greedily groups per-row frame sizes into records of at most |maxRecordBytes|, bound checked
//! before adding each row; a single row above the limit forms its own record. Returns per-record
//! row counts, in order.
std::vector<int> GroupKafkaRecordRows(const std::vector<i64>& frameSizes, i64 maxRecordBytes);

////////////////////////////////////////////////////////////////////////////////

//! One record queued for the producer thread.
struct TKafkaMessageToWrite
{
    i64 SeqNo = 0;
    std::optional<std::string> Key;
    //! Disengaged writes a Kafka null value (a compaction tombstone), distinct from an empty one.
    std::optional<std::string> Value;
    //! Value of the message id header, when the sink is configured to write one.
    std::optional<std::string> MessageId;
};

////////////////////////////////////////////////////////////////////////////////

//! The queue between the sink fibers and the writer's producer thread. Promises resolve strictly in
//! seqNo order (TOrderedAsyncSinkBase requires it; delivery reports arrive interleaved), and a
//! failed seqNo poisons the queue — every later seqNo fails even on an OK report, so the persisted
//! frontier never passes a message that did not reach Kafka and the replay re-sends it
//! (at-least-once: duplicates possible, loss not).
class TKafkaWriteQueue
{
public:
    //! Queues |message| and returns the future resolved once the broker acknowledges it; fails fast
    //! once the queue has failed.
    TFuture<void> Enqueue(TKafkaMessageToWrite&& message);

    //! Queues several records under one seqNo (a split batch), acknowledged together; the first
    //! failed record fails the whole seqNo.
    TFuture<void> Enqueue(i64 seqNo, std::vector<TKafkaMessageToWrite> records);

    //! Producer side. Hands over everything queued so far.
    std::deque<TKafkaMessageToWrite> TakePending();

    //! Producer side. Records the delivery result of |seqNo| and resolves the longest already-completed
    //! prefix of promises in seqNo order. Resolving an error poisons the queue (see the class comment).
    void Complete(i64 seqNo, TError error);

    //! Registers |seqNo| as failed although none of its records were enqueued (e.g. the batch failed
    //! to pack). A skipped seqNo would anchor the frontier past its messages or wedge later ones
    //! behind the gap.
    void Reject(i64 seqNo, TError error);

    //! Records a fatal error, fails every pending promise, and makes further Enqueue() calls fail fast
    //! so no promise is ever left unresolved.
    void Fail(const TError& error);

    //! The fatal (or poison) error, or OK while the queue is healthy.
    TError GetFatalError() const;

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    std::deque<TKafkaMessageToWrite> Pending_;
    THashMap<i64, TPromise<void>> Promises_;
    //! Outstanding record count for a multi-record seqNo; absent means a single record.
    THashMap<i64, i64> RemainingRecords_;
    //! Next seqNo whose promise may be resolved; delivery results for later seqNos wait for it.
    i64 NextSeqNoToResolve_ = -1;
    //! Delivery results that have arrived but cannot be resolved yet (a lower seqNo is still pending).
    std::map<i64, TError> CompletedResults_;
    //! Set once the writer can no longer make progress (e.g. producer construction failed); OK until then.
    TError FatalError_;
};

////////////////////////////////////////////////////////////////////////////////

//! Owns a single idempotent cppkafka::Producer on a dedicated thread: produces queued records and
//! serves delivery reports. Construction is retried with backoff; a failure afterwards poisons the
//! write queue (see #TKafkaWriteQueue).
class TRetryableKafkaWriter
    : public TRefCounted
{
public:
    using TMessageToWrite = TKafkaMessageToWrite;

    TRetryableKafkaWriter(
        TKafkaClientPtr client,
        std::string topic,
        std::string producerId,
        std::string messageIdHeader,
        NLogging::TLogger logger,
        IStatusProfilerPtr statusProfiler);

    void Start();
    void Terminate();

    TFuture<void> Write(TMessageToWrite&& message);
    TFuture<void> WriteMany(i64 seqNo, std::vector<TMessageToWrite> records);
    //! See #TKafkaWriteQueue::Reject.
    void Reject(i64 seqNo, TError error);

private:
    void Run();

    const TKafkaClientPtr Client_;
    const std::string Topic_;
    const std::string ProducerId_;
    //! Header name for the Flow message id; empty disables the header.
    const std::string MessageIdHeader_;
    const NLogging::TLogger Logger;
    const IStatusErrorStatePtr ErrorState_;

    NConcurrency::TActionQueuePtr WriteQueue_;
    std::atomic<bool> Terminated_ = false;

    TKafkaWriteQueue Queue_;
};

DEFINE_REFCOUNTED_TYPE(TRetryableKafkaWriter);

////////////////////////////////////////////////////////////////////////////////

//! Shared logic for all Kafka sink flavors: holds the client resource and the retryable writer, and
//! turns a (seqNo, payload, key) into a broker write returning a per-message future.
class TCommonKafkaSink
    : public virtual TRefCounted
{
public:
    TCommonKafkaSink(
        TSinkContextPtr context,
        TCommonKafkaSinkParametersPtr parameters,
        IStatusProfilerPtr statusProfiler,
        NLogging::TLogger logger);

    ~TCommonKafkaSink() override;

protected:
    const NLogging::TLogger Logger;

    void InitSession(const std::string& producerId);
    //! Extracts the payload (and optional key) column from the message and writes one Kafka record.
    TFuture<void> Write(const TOutputMessageConstPtr& message, i64 seqNo);
    //! Writes already-serialized values as records acknowledged together under one seqNo (for sinks
    //! packing several Flow messages into one record); no key or message id header — such a record
    //! has no single Flow message it speaks for.
    TFuture<void> WriteRecords(i64 seqNo, std::vector<TKafkaMessageToWrite> records);

    //! Registers |seqNo| as failed without writing anything, keeping the ordered frontier intact.
    void Reject(i64 seqNo, TError error);

    const std::string& PayloadColumn() const;

private:
    const TCommonKafkaSinkParametersPtr Parameters_;
    const TKafkaClientPtr Client_;
    const std::string Topic_;
    const IStatusProfilerPtr StatusProfiler_;
    TRetryableKafkaWriterPtr Writer_;
};

////////////////////////////////////////////////////////////////////////////////

//! Primary sink: per-message async writes with the idempotent producer. At-least-once (in-session
//! retries deduped by the broker; post-restart replays may duplicate downstream).
class TKafkaSink
    : public TOrderedAsyncSinkBase
    , public TCommonKafkaSink
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TKafkaSinkParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicKafkaSinkParameters);

    using TSinkController = TKafkaSinkController;

    TKafkaSink(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext);

private:
    using TCommonKafkaSink::Logger;

    void DoInit(const std::string& producerId) final;
    TFuture<void> DoDistribute(const TOutputMessageConstPtr& message, i64 seqNo) final;
};

DEFINE_REFCOUNTED_TYPE(TKafkaSink);

////////////////////////////////////////////////////////////////////////////////

//! Synchronous, simplest sink; at-least-once (fresh producer id per init, no persisted seqNo).
class TAtLeastOnceKafkaSink
    : public TSyncSinkBase
    , public TCommonKafkaSink
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TAtLeastOnceKafkaSinkParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicAtLeastOnceKafkaSinkParameters);

    using TSinkController = TKafkaSinkController;

    TAtLeastOnceKafkaSink(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext);

private:
    using TCommonKafkaSink::Logger;

    i64 SeqNo_ = 0;

    void DoInit() final;
    void DoDistribute(NApi::IDynamicTableTransactionPtr transaction, const std::deque<TOutputMessageConstPtr>& messages) final;
};

DEFINE_REFCOUNTED_TYPE(TAtLeastOnceKafkaSink);

////////////////////////////////////////////////////////////////////////////////

//! Minimal sink controller. librdkafka routes records to partitions, so the sink runs as a single
//! receiver channel for now.
class TKafkaSinkController
    : public TSinkControllerBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TKafkaSinkControllerParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicKafkaSinkControllerParameters);

    using TSinkControllerBase::TSinkControllerBase;

    std::optional<i64> GetReceiverChannelCount() override;
};

DEFINE_REFCOUNTED_TYPE(TKafkaSinkController);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
