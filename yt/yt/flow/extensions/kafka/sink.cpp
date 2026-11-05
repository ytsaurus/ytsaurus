#include "sink.h"

#include "helpers.h"
#include "kafka_client.h"
#include "private.h"
#include "transactional_writer.h"

#include <yt/yt/flow/library/cpp/common/message.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/guid.h>

#include <contrib/libs/cppkafka/include/cppkafka/buffer.h>
#include <contrib/libs/cppkafka/include/cppkafka/configuration.h>
#include <contrib/libs/cppkafka/include/cppkafka/exceptions.h>
#include <contrib/libs/cppkafka/include/cppkafka/header.h>
#include <contrib/libs/cppkafka/include/cppkafka/message.h>
#include <contrib/libs/cppkafka/include/cppkafka/message_builder.h>
#include <contrib/libs/cppkafka/include/cppkafka/producer.h>

#include <librdkafka/rdkafka.h>

#include <util/datetime/base.h>

#include <chrono>

namespace NYT::NFlow {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr auto ProducerPollTimeout = std::chrono::milliseconds(100);
constexpr auto ProducerFlushTimeout = std::chrono::seconds(30);
constexpr int MaxProduceRetries = 100;

void* EncodeSeqNo(i64 seqNo)
{
    return reinterpret_cast<void*>(static_cast<intptr_t>(seqNo));
}

i64 DecodeSeqNo(void* userData)
{
    return static_cast<i64>(reinterpret_cast<intptr_t>(userData));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

i64 TKafkaMessageToWrite::GetByteSize() const
{
    return (Key ? std::ssize(*Key) : 0) + (Value ? std::ssize(*Value) : 0) + (MessageId ? std::ssize(*MessageId) : 0);
}

void ProduceKafkaMessage(cppkafka::Producer& producer, const cppkafka::MessageBuilder& builder)
{
    for (int retry = 0;; ++retry) {
        try {
            producer.produce(builder);
            return;
        } catch (const cppkafka::HandleException& ex) {
            if (ex.get_error().get_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL && retry < MaxProduceRetries) {
                producer.poll(ProducerPollTimeout);
                continue;
            }
            throw;
        }
    }
}

cppkafka::MessageBuilder MakeKafkaMessageBuilder(
    const std::string& topic,
    const TKafkaMessageToWrite& record,
    const std::string& messageIdHeader)
{
    cppkafka::MessageBuilder builder(topic);
    if (record.Value) {
        builder.payload(cppkafka::Buffer(*record.Value));
    }
    if (record.Key) {
        builder.key(cppkafka::Buffer(*record.Key));
    }
    if (record.MessageId && !messageIdHeader.empty()) {
        builder.header(cppkafka::Header<cppkafka::Buffer>(
            messageIdHeader,
            cppkafka::Buffer(*record.MessageId)));
    }
    return builder;
}

////////////////////////////////////////////////////////////////////////////////

TRetryableKafkaWriter::TRetryableKafkaWriter(
    TKafkaClientPtr client,
    std::string topic,
    std::string producerId,
    std::string messageIdHeader,
    NLogging::TLogger logger,
    IStatusProfilerPtr statusProfiler)
    : Client_(std::move(client))
    , Topic_(std::move(topic))
    , ProducerId_(std::move(producerId))
    , MessageIdHeader_(std::move(messageIdHeader))
    , Logger(std::move(logger))
    , ErrorState_(statusProfiler->ErrorState("writer"))
{ }

void TRetryableKafkaWriter::Start()
{
    WriteQueue_ = New<TActionQueue>("KafkaWrite");
    WriteQueue_->GetInvoker()->Invoke(BIND(&TRetryableKafkaWriter::Run, MakeWeak(this)));
}

void TRetryableKafkaWriter::Terminate()
{
    Terminated_.store(true);
    if (WriteQueue_) {
        WriteQueue_->Shutdown(/*graceful*/ true);
        WriteQueue_.Reset();
    }
}

////////////////////////////////////////////////////////////////////////////////

std::vector<int> GroupKafkaRecordRows(const std::vector<i64>& frameSizes, i64 maxRecordBytes)
{
    std::vector<int> counts;
    i64 recordBytes = 0;
    int recordRows = 0;
    for (auto frameSize : frameSizes) {
        if (recordRows > 0 && recordBytes + frameSize > maxRecordBytes) {
            counts.push_back(recordRows);
            recordRows = 0;
            recordBytes = 0;
        }
        ++recordRows;
        recordBytes += frameSize;
    }
    if (recordRows > 0) {
        counts.push_back(recordRows);
    }
    return counts;
}

////////////////////////////////////////////////////////////////////////////////

void TKafkaWriteQueue::Enqueue(std::vector<TKafkaMessageToWrite> records, TOnWritten onWritten)
{
    YT_VERIFY(!records.empty());

    TError error;
    {
        auto guard = Guard(Lock_);
        error = FatalError_;
        if (error.IsOK()) {
            i64 seqNo = FirstSeqNo_ + std::ssize(Completions_);
            Completions_.push_back({std::ssize(records), std::move(onWritten)});
            for (auto& record : records) {
                record.SeqNo = seqNo;
                Pending_.push_back(std::move(record));
            }
            return;
        }
    }
    onWritten(error);
}

std::deque<TKafkaMessageToWrite> TKafkaWriteQueue::TakePending()
{
    auto guard = Guard(Lock_);
    return std::exchange(Pending_, {});
}

std::deque<TKafkaMessageToWrite> TKafkaWriteQueue::TakePending(i64 maxCount, i64 maxByteSize)
{
    YT_VERIFY(maxCount > 0);
    std::deque<TKafkaMessageToWrite> taken;
    i64 byteSize = 0;
    auto guard = Guard(Lock_);
    while (!Pending_.empty() && std::ssize(taken) < maxCount) {
        auto recordByteSize = Pending_.front().GetByteSize();
        if (!taken.empty() && byteSize + recordByteSize > maxByteSize) {
            break;
        }
        byteSize += recordByteSize;
        taken.push_back(std::move(Pending_.front()));
        Pending_.pop_front();
    }
    return taken;
}

void TKafkaWriteQueue::Complete(i64 seqNo, TError error)
{
    std::vector<TOnWritten> completed;
    {
        auto guard = Guard(Lock_);
        i64 index = seqNo - FirstSeqNo_;
        if (!FatalError_.IsOK() || index < 0 || index >= std::ssize(Completions_)) {
            return;
        }
        if (error.IsOK()) {
            auto& remaining = Completions_[index].RemainingRecords;
            if (remaining == 0) {
                return;
            }

            --remaining;
            while (!Completions_.empty() && Completions_.front().RemainingRecords == 0) {
                completed.push_back(std::move(Completions_.front().OnWritten));
                Completions_.pop_front();
                ++FirstSeqNo_;
            }
        }
    }

    if (!error.IsOK()) {
        Fail(error);
    }

    for (auto& onWritten : completed) {
        onWritten(TError());
    }
}

TError TKafkaWriteQueue::GetFatalError() const
{
    auto guard = Guard(Lock_);
    return FatalError_;
}

void TKafkaWriteQueue::Fail(const TError& error)
{
    std::deque<TCompletion> failed;
    std::deque<TKafkaMessageToWrite> pending;
    {
        auto guard = Guard(Lock_);
        if (!FatalError_.IsOK()) {
            return;
        }

        FatalError_ = error;
        failed = std::exchange(Completions_, {});
        pending = std::exchange(Pending_, {});
    }

    for (auto& completion : failed) {
        completion.OnWritten(error);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TRetryableKafkaWriter::WriteMany(std::vector<TMessageToWrite> records, TKafkaWriteQueue::TOnWritten onWritten)
{
    Queue_.Enqueue(std::move(records), std::move(onWritten));
}

TError TRetryableKafkaWriter::GetFatalError() const
{
    return Queue_.GetFatalError();
}

void TRetryableKafkaWriter::Run()
{
    // Retrying construction is free (nothing produced yet, enqueued writes wait); exiting would
    // leave the sink dead until the job is replaced.
    auto producer = CreateUntilTerminated<cppkafka::Producer>(
        [&] {
            auto configuration = Client_->MakeBaseConfiguration();
            configuration.set("enable.idempotence", "true");
            configuration.set("client.id", ProducerId_);
            configuration.set_delivery_report_callback(
                [this] (cppkafka::Producer& /*producer*/, const cppkafka::Message& message) {
                    auto seqNo = DecodeSeqNo(message.get_user_data());
                    if (auto error = message.get_error()) {
                        Queue_.Complete(seqNo, TError("Kafka delivery failed: %v", error.to_string()));
                    } else {
                        Queue_.Complete(seqNo, TError());
                    }
                });
            return std::make_unique<cppkafka::Producer>(std::move(configuration));
        },
        [&] (const std::exception& ex) {
            YT_TLOG_ERROR("Failed to create Kafka producer")
                .With(ex);
            ErrorState_->SetError(TError("Failed to create Kafka producer").With(ex));
        },
        Terminated_,
        KafkaHandleCreateBackoff);
    if (!producer) {
        // Fail queued and future writes when Run() exits early.
        Queue_.Fail(TError("Kafka writer terminated before a producer could be created"));
        return;
    }

    while (!Terminated_.load()) {
        auto batch = Queue_.TakePending();

        bool produceFailed = false;
        for (const auto& message : batch) {
            try {
                auto builder = MakeKafkaMessageBuilder(Topic_, message, MessageIdHeader_);
                builder.user_data(EncodeSeqNo(message.SeqNo));

                ProduceKafkaMessage(*producer, builder);
            } catch (const std::exception& ex) {
                // produce() failed for this record; the delivery-report path will never fire for it,
                // so report the failure here.
                auto error = TError("Failed to produce Kafka message").With(ex);
                YT_TLOG_ERROR("Offset dropped")
                    .With("SeqNo", message.SeqNo)
                    .With(error);
                ErrorState_->SetError(error);
                produceFailed = true;
                Queue_.Complete(message.SeqNo, error);
            }
        }

        try {
            producer->poll(ProducerPollTimeout);
            // A successful poll alone does not mean the writer is healthy: a produce failure earlier
            // in this iteration, or a poisoned queue, must keep the sensor red.
            if (auto fatalError = Queue_.GetFatalError(); !fatalError.IsOK()) {
                ErrorState_->SetError(fatalError);
            } else if (!produceFailed) {
                ErrorState_->ClearError();
            }
        } catch (const std::exception& ex) {
            auto error = TError("Kafka producer poll failed").With(ex);
            YT_TLOG_ERROR("Kafka producer poll failed")
                .With(ex);
            ErrorState_->SetError(error);
        }
    }

    // Drain outstanding acknowledgements, then fail anything still queued.
    try {
        producer->flush(ProducerFlushTimeout);
    } catch (const std::exception& ex) {
        YT_TLOG_WARNING("Failed to flush Kafka producer on terminate")
            .With(ex);
    }
    Queue_.Fail(TError("Kafka writer terminated before the message was acknowledged"));
}

////////////////////////////////////////////////////////////////////////////////

TCommonKafkaSink::TCommonKafkaSink(
    TSinkContextPtr context,
    TCommonKafkaSinkParametersPtr parameters,
    IStatusProfilerPtr statusProfiler,
    NLogging::TLogger logger)
    : Logger(logger.WithTag("Topic", parameters->Topic))
    , Parameters_(std::move(parameters))
    , Client_(context->GetStaticResource(KafkaClientDefaultResourceId)->As<TKafkaClient>())
    , Topic_(Parameters_->Topic)
    , StatusProfiler_(std::move(statusProfiler))
{ }

TCommonKafkaSink::~TCommonKafkaSink()
{
    TerminateWriter();
}

void TCommonKafkaSink::TerminateWriter()
{
    if (Writer_) {
        // Terminate joins the writer thread (up to the final flush timeout); never block here — the
        // destructor runs on a fiber scheduler thread. The ref keeps the writer alive.
        GetFinalizerInvoker()->Invoke(BIND([writer = std::move(Writer_)] {
            writer->Terminate();
        }));
    }
}

void TCommonKafkaSink::InitSession(const std::string& producerId)
{
    Writer_ = New<TRetryableKafkaWriter>(
        Client_,
        Topic_,
        producerId,
        Parameters_->MessageIdHeader,
        Logger.WithTag("Session", producerId),
        StatusProfiler_->WithPrefix("/session"));
    Writer_->Start();
}

namespace {

//! GetColumnValue<std::string> collapses a null column into an empty string; Kafka distinguishes the
//! two (null key = round-robin routing, null value = compaction tombstone), so extract presence
//! explicitly.
std::optional<std::string> GetOptionalColumnValue(const TOutputMessage& message, const std::string& columnName)
{
    auto value = GetColumn(message, columnName);
    if (value.Type == NTableClient::EValueType::Null) {
        return std::nullopt;
    }
    return NTableClient::FromUnversionedValue<std::string>(value);
}

} // namespace

TKafkaMessageToWrite TCommonKafkaSink::MakeRecord(const TOutputMessageConstPtr& message, bool includeMessageId) const
{
    auto value = GetOptionalColumnValue(*message, Parameters_->PayloadColumn);
    std::optional<std::string> key;
    if (!Parameters_->KeyColumn.empty()) {
        key = GetOptionalColumnValue(*message, Parameters_->KeyColumn);
    }
    // The message id is deterministic across replays, so a consumer can use the header to drop the
    // duplicates the at-least-once sink may produce after a restart.
    std::optional<std::string> messageId;
    if (includeMessageId || !Parameters_->MessageIdHeader.empty()) {
        messageId = std::string(message->GetMeta().MessageId.Underlying());
    }
    return {
        .Key = std::move(key),
        .Value = std::move(value),
        .MessageId = std::move(messageId),
    };
}

const std::string& TCommonKafkaSink::PayloadColumn() const
{
    return Parameters_->PayloadColumn;
}

const TKafkaClientPtr& TCommonKafkaSink::GetClient() const
{
    return Client_;
}

////////////////////////////////////////////////////////////////////////////////

void TKafkaSinkState::Register(TRegistrar registrar)
{
    registrar.Parameter("max_distributed_seq_no", &TThis::MaxDistributedSeqNo)
        .Default(0);
    registrar.Parameter("max_distributed_message_id", &TThis::MaxDistributedMessageId)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TKafkaSink::TKafkaSink(
    TSinkContextPtr context,
    TDynamicSinkContextPtr dynamicContext)
    : TSinkBase(std::move(context), std::move(dynamicContext))
    , TCommonKafkaSink(GetContext(), DynamicPointerCast<TCommonKafkaSinkParameters>(GetParametersBase()), GetContext()->StatusProfiler, TSinkBase::Logger)
{ }

TKafkaSink::~TKafkaSink()
{
    DoTerminate();
}

void TKafkaSink::DoTerminate()
{
    Terminated_ = true;
    Pending_.clear();
    TerminateWriter();
    if (auto writer = std::exchange(TransactionalWriter_, {})) {
        GetFinalizerInvoker()->Invoke(BIND([writer = std::move(writer)] {
            writer->Terminate();
        }));
    }
}

void TKafkaSink::Init(IInitContextPtr initContext)
{
    THROW_ERROR_EXCEPTION_UNLESS(GetSpec()->InputStreamIds.size() == 1, "Kafka sink input stream count is invalid: expected 1, actual %v", std::ssize(GetSpec()->InputStreamIds));

    initContext->InitClient(State_, "v0");
    if (State_->ProducerId.empty()) {
        State_->ProducerId = ToString(TGuid::Create());
    }
    LastMessageId_ = AcknowledgedMessageId_ = ReleaseMessageId_ = State_->MaxPersistedMessageId;
    InitWriter(std::move(initContext));
}

void TKafkaSink::InitWriter(IInitContextPtr initContext)
{
    initContext->InitClient(TransactionalState_, "kafka_v0");
    const auto& parameters = GetParameters();
    if (parameters->DeliveryGuarantee == EKafkaDeliveryGuarantee::AtLeastOnce) {
        TransactionalState_.Clear();
        InitSession(State_->ProducerId);
        return;
    }

    TransactionalWriter_ = New<TTransactionalKafkaWriter>(
        GetClient(),
        TTransactionalKafkaWriterOptions{
            .Topic = parameters->Topic,
            .TransactionalId = parameters->TransactionalIdPrefix + State_->ProducerId,
            .MessageIdHeader = parameters->MessageIdHeader,
            .TransactionTimeout = parameters->TransactionTimeout,
            .MaxTransactionRecordCount = parameters->MaxTransactionRecordCount,
            .MaxTransactionByteSize = parameters->MaxTransactionByteSize,
            .AllowMissingProgressMarker = GetDynamicParameters()->AllowMissingProgressMarker,
        },
        TKafkaTransactionalRecovery{
            .MaxPersistedMessageId = AcknowledgedMessageId_,
            .MaxDistributedMessageId = TransactionalState_->MaxDistributedMessageId,
            .MaxPersistedSeqNo = State_->MaxPersistedSeqNo,
            .MaxDistributedSeqNo = TransactionalState_->MaxDistributedSeqNo,
        },
        Logger,
        GetContext()->StatusProfiler->WithPrefix("/session"));
    TransactionalWriter_->Start();
}

void TKafkaSink::ThrowIfFailed() const
{
    THROW_ERROR_EXCEPTION_IF(Terminated_, "Kafka sink terminated");

    Error_.ThrowOnError();
    (TransactionalWriter_ ? TransactionalWriter_->GetFatalError() : Writer_->GetFatalError())
        .ThrowOnError();
}

void TKafkaSink::Distribute(const TOutputMessageConstPtr& message, TOnDistributedCallback onDistributed)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);
    ThrowIfFailed();

    YT_TLOG_DEBUG("MessageLifeCycle.Sink: message was registered")
        .With("MessageId", message->MessageId)
        .With("StreamId", message->StreamId)
        .With("SystemTimestamp", message->SystemTimestamp)
        .With("EventTimestamp", message->EventTimestamp)
        .With("ByteSize", message->ByteSize);

    if (message->MessageId <= State_->MaxPersistedMessageId) {
        onDistributed();
        return;
    }

    THROW_ERROR_EXCEPTION_UNLESS(message->MessageId > LastMessageId_, "Kafka sink message IDs must strictly increase: expected greater than %v, actual %v", LastMessageId_, message->MessageId);

    LastMessageId_ = message->MessageId;
    Pending_.push_back({message, std::move(onDistributed)});
}

void TKafkaSink::Sync(NApi::IDynamicTableTransactionPtr /*transaction*/)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);
    ThrowIfFailed();
    State_->MaxPersistedMessageId = AcknowledgedMessageId_;
    if (!TransactionalWriter_) {
        ReleaseMessageId_ = LastMessageId_;
    } else if (auto started = TransactionalWriter_->GetStarted().TryGet(); started && started->IsOK()) {
        // Startup establishes the marker; only a successful checkpoint may then release this bound.
        ReleaseMessageId_ = std::max(TransactionalState_->MaxDistributedMessageId.value_or(TMessageId()), LastMessageId_);
        TransactionalState_->MaxDistributedMessageId = ReleaseMessageId_;
        TransactionalState_->MaxDistributedSeqNo = 0;
    }
}

void TKafkaSink::Commit()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);
    ThrowIfFailed();
    try {
        std::deque<TPendingMessage> pending;
        std::vector<TKafkaMessageToWrite> records;
        while (!Pending_.empty() && Pending_.front().Message->MessageId <= ReleaseMessageId_) {
            auto& item = Pending_.front();
            records.push_back(MakeRecord(item.Message, /*includeMessageId*/ TransactionalWriter_ != nullptr));
            pending.push_back(std::move(item));
            Pending_.pop_front();
        }

        if (pending.empty()) {
            return;
        }

        auto onWritten = BIND([weakThis = MakeWeak(this), pending = std::move(pending)] (const TError& error) mutable {
            if (auto sink = weakThis.Lock(); sink && !sink->Terminated_ && error.IsOK()) {
                sink->AcknowledgedMessageId_ = pending.back().Message->MessageId;
                for (auto& item : pending) {
                    sink->ObserveEventLag(item.Message->StreamId, item.Message->EventTimestamp);
                    item.OnDistributed();
                }
            }
        }).Via(GetContext()->SerializedInvoker);

        if (TransactionalWriter_) {
            TransactionalWriter_->WriteMany(std::move(records), std::move(onWritten));
        } else {
            Writer_->WriteMany(std::move(records), std::move(onWritten));
        }
    } catch (const std::exception& ex) {
        Error_ = TError("Failed to dispatch Kafka message").With(ex);
        Error_.ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

TAtLeastOnceKafkaSink::TAtLeastOnceKafkaSink(
    TSinkContextPtr context,
    TDynamicSinkContextPtr dynamicContext)
    : TSyncSinkBase(std::move(context), std::move(dynamicContext))
    , TCommonKafkaSink(GetContext(), GetParameters(), GetContext()->StatusProfiler, TSyncSinkBase::Logger)
{ }

void TAtLeastOnceKafkaSink::DoInit()
{
    InitSession(ToString(TGuid::Create()));
}

void TAtLeastOnceKafkaSink::DoDistribute(
    NApi::IDynamicTableTransactionPtr /*transaction*/,
    const std::deque<TOutputMessageConstPtr>& messages)
{
    if (messages.empty()) {
        return;
    }

    std::vector<TKafkaMessageToWrite> records;
    records.reserve(messages.size());
    for (const auto& message : messages) {
        records.push_back(MakeRecord(message));
    }

    auto promise = NewPromise<void>();
    Writer_->WriteMany(std::move(records), BIND([promise] (const TError& error) {
        promise.Set(error);
    }));
    WaitFor(promise.ToFuture())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

std::optional<i64> TKafkaSinkController::GetReceiverChannelCount()
{
    // librdkafka routes records to partitions; a single receiver channel is sufficient for now.
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
