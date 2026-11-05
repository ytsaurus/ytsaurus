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

#include <library/cpp/yt/system/spin_lock.h>

#include <contrib/libs/cppkafka/include/cppkafka/message_builder.h>

#include <deque>
#include <memory>

namespace cppkafka {

class Producer;

} // namespace cppkafka

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
    //! Assigned by the queue; never persisted or used for deduplication.
    i64 SeqNo = 0;
    std::optional<std::string> Key;
    //! Disengaged writes a Kafka null value (a compaction tombstone), distinct from an empty one.
    std::optional<std::string> Value;
    //! Value of the message id header, when the sink is configured to write one.
    std::optional<std::string> MessageId;

    //! The bytes of the key, the value and the message id.
    i64 GetByteSize() const;
};

//! The Kafka message for |record|; an empty |messageIdHeader| leaves the message id out.
cppkafka::MessageBuilder MakeKafkaMessageBuilder(
    const std::string& topic,
    const TKafkaMessageToWrite& record,
    const std::string& messageIdHeader);

//! Produces |builder|, retrying only while the local queue is full (serving delivery reports meanwhile);
//! throws on any other error.
void ProduceKafkaMessage(cppkafka::Producer& producer, const cppkafka::MessageBuilder& builder);

////////////////////////////////////////////////////////////////////////////////

//! Thread-safe producer handoff with ordered, all-record request completion.
class TKafkaWriteQueue
{
public:
    using TOnWritten = TCallback<void(const TError&)>;

    void Enqueue(std::vector<TKafkaMessageToWrite> records, TOnWritten onWritten);
    std::deque<TKafkaMessageToWrite> TakePending();
    //! Takes a bounded prefix, including at least one queued record.
    std::deque<TKafkaMessageToWrite> TakePending(i64 maxCount, i64 maxByteSize);
    void Complete(i64 seqNo, TError error);
    void Fail(const TError& error);
    TError GetFatalError() const;

private:
    struct TCompletion
    {
        i64 RemainingRecords = 0;
        TOnWritten OnWritten;
    };

    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    std::deque<TKafkaMessageToWrite> Pending_;
    std::deque<TCompletion> Completions_;
    i64 FirstSeqNo_ = 1;
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

    void WriteMany(std::vector<TMessageToWrite> records, TKafkaWriteQueue::TOnWritten onWritten);

    //! See #TKafkaWriteQueue::GetFatalError().
    TError GetFatalError() const;

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

//! Persisted state of #TKafkaSink with #EKafkaDeliveryGuarantee::ExactlyOnce.
struct TKafkaSinkState
    : public NYTree::TYsonStruct
{
    //! Retained only to detect ambiguous writes when migrating legacy state.
    i64 MaxDistributedSeqNo = 0;
    //! The ID up to which earlier sessions may have committed messages.
    std::optional<TMessageId> MaxDistributedMessageId;

    REGISTER_YSON_STRUCT(TKafkaSinkState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TKafkaSinkState);

////////////////////////////////////////////////////////////////////////////////

//! Shared resource, record conversion and nontransactional writer for Kafka sinks.
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

    TRetryableKafkaWriterPtr Writer_;

    void InitSession(const std::string& producerId);
    TKafkaMessageToWrite MakeRecord(const TOutputMessageConstPtr& message, bool includeMessageId = false) const;
    void TerminateWriter();

    const std::string& PayloadColumn() const;
    const TKafkaClientPtr& GetClient() const;

private:
    const TCommonKafkaSinkParametersPtr Parameters_;
    const TKafkaClientPtr Client_;
    const std::string Topic_;
    const IStatusProfilerPtr StatusProfiler_;
};

////////////////////////////////////////////////////////////////////////////////

//! Primary sink: asynchronous writes. Exactly-once by default (see #TTransactionalKafkaWriter);
//! at-least-once with #EKafkaDeliveryGuarantee::AtLeastOnce, where the idempotent producer drops the
//! retries within a session but not the replays after a restart.
class TKafkaSink
    : public TSinkBase
    , public TCommonKafkaSink
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TKafkaSinkParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicKafkaSinkParameters);

    using TSinkController = TKafkaSinkController;

    TKafkaSink(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext);

    ~TKafkaSink() override;

    void Init(IInitContextPtr initContext) override;
    void Distribute(const TOutputMessageConstPtr& message, TOnDistributedCallback onDistributed) override;
    void Sync(NApi::IDynamicTableTransactionPtr transaction) override;
    void Commit() override;

protected:
    using TCommonKafkaSink::Logger;

    struct TPendingMessage
    {
        TOutputMessageConstPtr Message;
        TOnDistributedCallback OnDistributed;
    };

    TMutableStateClient<TOrderedAsyncSinkState> State_;
    TMessageId AcknowledgedMessageId_;
    TMessageId ReleaseMessageId_;
    std::deque<TPendingMessage> Pending_;
    bool Terminated_ = false;
    TError Error_;

    virtual void InitWriter(IInitContextPtr initContext);
    void ThrowIfFailed() const;

private:
    TTransactionalKafkaWriterPtr TransactionalWriter_;
    TMutableStateClient<TKafkaSinkState> TransactionalState_;
    TMessageId LastMessageId_;

    void DoTerminate() override;
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
