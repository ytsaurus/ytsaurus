#include "sink.h"

#include "helpers.h"
#include "kafka_client.h"
#include "private.h"

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
#include <limits>

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

//! Produces one message, retrying only the transient "local queue full" error (draining delivery
//! reports between attempts). Any other error is thrown immediately instead of being retried blindly.
void ProduceMessage(cppkafka::Producer& producer, const cppkafka::MessageBuilder& builder)
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

} // namespace

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

TFuture<void> TKafkaWriteQueue::Enqueue(TKafkaMessageToWrite&& message)
{
    auto seqNo = message.SeqNo;
    std::vector<TKafkaMessageToWrite> records;
    records.push_back(std::move(message));
    return Enqueue(seqNo, std::move(records));
}

TFuture<void> TKafkaWriteQueue::Enqueue(i64 seqNo, std::vector<TKafkaMessageToWrite> records)
{
    YT_VERIFY(!records.empty());

    auto promise = NewPromise<void>();
    {
        auto guard = Guard(Lock_);
        if (!FatalError_.IsOK()) {
            // The writer can no longer make progress; fail fast instead of queueing a promise nothing
            // will ever resolve.
            return MakeFuture<void>(FatalError_);
        }
        if (NextSeqNoToResolve_ < 0) {
            NextSeqNoToResolve_ = seqNo;
        }
        EmplaceOrCrash(Promises_, seqNo, promise);
        if (std::ssize(records) > 1) {
            EmplaceOrCrash(RemainingRecords_, seqNo, std::ssize(records));
        }
        for (auto& record : records) {
            Pending_.push_back(std::move(record));
        }
    }
    return promise.ToFuture();
}

std::deque<TKafkaMessageToWrite> TKafkaWriteQueue::TakePending()
{
    auto guard = Guard(Lock_);
    return std::exchange(Pending_, {});
}

void TKafkaWriteQueue::Complete(i64 seqNo, TError error)
{
    std::vector<std::pair<TPromise<void>, TError>> toResolve;
    std::vector<TPromise<void>> poisoned;
    TError poison;
    {
        auto guard = Guard(Lock_);
        if (!FatalError_.IsOK()) {
            // Every promise has already been failed; ignore late delivery reports instead of
            // accumulating their results forever.
            return;
        }
        if (error.IsOK()) {
            // A multi-record seqNo completes only when its last record is acknowledged.
            if (auto it = RemainingRecords_.find(seqNo); it != RemainingRecords_.end()) {
                if (--it->second > 0) {
                    return;
                }
                RemainingRecords_.erase(it);
            }
            // The first failure is sticky: a later OK report (another record of the split batch)
            // must not overwrite it, or the base would persist past a record that never reached Kafka.
            if (!CompletedResults_.emplace(seqNo, TError()).second) {
                return;
            }
        } else {
            // The first failed record fails the whole seqNo. An error may overwrite a stored OK
            // (the replay re-sends), never the other way around.
            RemainingRecords_.erase(seqNo);
            CompletedResults_[seqNo] = std::move(error);
        }
        if (NextSeqNoToResolve_ < 0) {
            NextSeqNoToResolve_ = seqNo;
        }
        // Resolve the contiguous run of completed seqNos in order; a gap means an earlier write is still
        // in flight, and later completions must wait for it.
        while (true) {
            auto it = CompletedResults_.find(NextSeqNoToResolve_);
            if (it == CompletedResults_.end()) {
                break;
            }
            auto resolvedSeqNo = it->first;
            auto result = std::move(it->second);
            CompletedResults_.erase(it);
            ++NextSeqNoToResolve_;

            bool failed = !result.IsOK();
            if (failed) {
                // Everything after a failed seqNo must fail too, or the sink base would persist a
                // message id past the failure and deduplicate its replay away (see the class comment).
                poison = TError("Kafka write failed; failing subsequent writes so the job replay re-sends them")
                    .With(result);
            }
            if (auto promiseIt = Promises_.find(resolvedSeqNo); promiseIt != Promises_.end()) {
                toResolve.emplace_back(promiseIt->second, std::move(result));
                Promises_.erase(promiseIt);
            }
            if (failed) {
                break;
            }
        }
        if (!poison.IsOK()) {
            FatalError_ = poison;
            poisoned.reserve(Promises_.size());
            for (auto& [pendingSeqNo, promise] : Promises_) {
                poisoned.push_back(std::move(promise));
            }
            Promises_.clear();
            Pending_.clear();
            CompletedResults_.clear();
            RemainingRecords_.clear();
        }
    }
    for (auto& [promise, resolvedError] : toResolve) {
        promise.TrySet(std::move(resolvedError));
    }
    for (auto& promise : poisoned) {
        promise.TrySet(poison);
    }
}

void TKafkaWriteQueue::Reject(i64 seqNo, TError error)
{
    YT_VERIFY(!error.IsOK());
    // Complete already has rejection semantics: it anchors an unseen seqNo, stores the error, and
    // poisons at resolution.
    Complete(seqNo, std::move(error));
}

TError TKafkaWriteQueue::GetFatalError() const
{
    auto guard = Guard(Lock_);
    return FatalError_;
}

void TKafkaWriteQueue::Fail(const TError& error)
{
    std::vector<TPromise<void>> orphaned;
    {
        auto guard = Guard(Lock_);
        FatalError_ = error;
        orphaned.reserve(Promises_.size());
        for (auto& [seqNo, promise] : Promises_) {
            orphaned.push_back(std::move(promise));
        }
        Promises_.clear();
        Pending_.clear();
        CompletedResults_.clear();
        RemainingRecords_.clear();
    }
    for (auto& promise : orphaned) {
        promise.TrySet(error);
    }
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TRetryableKafkaWriter::Write(TMessageToWrite&& message)
{
    return Queue_.Enqueue(std::move(message));
}

TFuture<void> TRetryableKafkaWriter::WriteMany(i64 seqNo, std::vector<TMessageToWrite> records)
{
    return Queue_.Enqueue(seqNo, std::move(records));
}

void TRetryableKafkaWriter::Reject(i64 seqNo, TError error)
{
    Queue_.Reject(seqNo, std::move(error));
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
        // Fail every queued and future write so no promise is orphaned when Run() exits early.
        Queue_.Fail(TError("Kafka writer terminated before a producer could be created"));
        return;
    }

    while (!Terminated_.load()) {
        auto batch = Queue_.TakePending();

        bool produceFailed = false;
        for (const auto& message : batch) {
            try {
                cppkafka::MessageBuilder builder(Topic_);
                if (message.Value) {
                    builder.payload(cppkafka::Buffer(*message.Value));
                }
                if (message.Key) {
                    builder.key(cppkafka::Buffer(*message.Key));
                }
                if (message.MessageId) {
                    builder.header(cppkafka::Header<cppkafka::Buffer>(
                        MessageIdHeader_,
                        cppkafka::Buffer(*message.MessageId)));
                }
                builder.user_data(EncodeSeqNo(message.SeqNo));

                ProduceMessage(*producer, builder);
            } catch (const std::exception& ex) {
                // produce() failed for this record; the delivery-report path will never fire for it,
                // so resolve its promise here instead of dropping the rest of the batch.
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

TFuture<void> TCommonKafkaSink::Write(const TOutputMessageConstPtr& message, i64 seqNo)
{
    auto value = GetOptionalColumnValue(*message, Parameters_->PayloadColumn);
    std::optional<std::string> key;
    if (!Parameters_->KeyColumn.empty()) {
        key = GetOptionalColumnValue(*message, Parameters_->KeyColumn);
    }
    // The message id is deterministic across replays, so a consumer can use the header to drop the
    // duplicates the at-least-once sink may produce after a restart.
    std::optional<std::string> messageId;
    if (!Parameters_->MessageIdHeader.empty()) {
        messageId = std::string(message->GetMeta().MessageId.Underlying());
    }
    return Writer_->Write({
        .SeqNo = seqNo,
        .Key = std::move(key),
        .Value = std::move(value),
        .MessageId = std::move(messageId),
    });
}

TFuture<void> TCommonKafkaSink::WriteRecords(i64 seqNo, std::vector<TKafkaMessageToWrite> records)
{
    return Writer_->WriteMany(seqNo, std::move(records));
}

void TCommonKafkaSink::Reject(i64 seqNo, TError error)
{
    Writer_->Reject(seqNo, std::move(error));
}

const std::string& TCommonKafkaSink::PayloadColumn() const
{
    return Parameters_->PayloadColumn;
}

////////////////////////////////////////////////////////////////////////////////

TKafkaSink::TKafkaSink(
    TSinkContextPtr context,
    TDynamicSinkContextPtr dynamicContext)
    : TOrderedAsyncSinkBase(std::move(context), std::move(dynamicContext))
    , TCommonKafkaSink(GetContext(), GetParameters(), GetContext()->StatusProfiler, TOrderedAsyncSinkBase::Logger)
{ }

void TKafkaSink::DoInit(const std::string& producerId)
{
    InitSession(producerId);
}

TFuture<void> TKafkaSink::DoDistribute(const TOutputMessageConstPtr& message, i64 seqNo)
{
    return Write(message, seqNo);
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
    std::vector<TFuture<void>> futures;
    futures.reserve(messages.size());
    for (const auto& message : messages) {
        futures.push_back(Write(message, ++SeqNo_));
    }
    for (auto& future : futures) {
        WaitFor(future).ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

std::optional<i64> TKafkaSinkController::GetReceiverChannelCount()
{
    // librdkafka routes records to partitions; a single receiver channel is sufficient for now.
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
