#include "transactional_writer.h"

#include "helpers.h"
#include "kafka_client.h"

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/finally.h>

#include <contrib/libs/cppkafka/include/cppkafka/configuration.h>
#include <contrib/libs/cppkafka/include/cppkafka/consumer.h>
#include <contrib/libs/cppkafka/include/cppkafka/exceptions.h>
#include <contrib/libs/cppkafka/include/cppkafka/producer.h>

#include <librdkafka/rdkafka.h>

#include <array>
#include <chrono>

namespace NYT::NFlow {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr auto ProducerPollTimeout = std::chrono::milliseconds(100);
//! Timeout of a single blocking transaction or offset request.
constexpr auto TransactionRequestTimeout = std::chrono::seconds(30);
constexpr auto TransactionAbortOnTerminateTimeout = std::chrono::seconds(5);
//! The marker is the consumer group's offset on this partition of the topic.
constexpr int ProgressMarkerPartition = 0;

int ToTimeoutMs(std::chrono::milliseconds timeout)
{
    return static_cast<int>(timeout.count());
}

//! Produce errors that repeating the same record cannot fix.
bool IsPermanentProduceError(rd_kafka_resp_err_t error)
{
    switch (error) {
        case RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE:
        case RD_KAFKA_RESP_ERR__INVALID_ARG:
        case RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC:
        case RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED:
            return true;
        default:
            return false;
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TErrorOr<std::optional<i64>> ResolveKafkaProgressMarker(
    const std::optional<TKafkaCommittedOffset>& committed,
    const TKafkaTransactionalRecovery& recovery)
{
    auto maxPersistedSeqNo = recovery.MaxPersistedSeqNo;
    auto maxDistributedSeqNo = std::max(recovery.MaxDistributedSeqNo, maxPersistedSeqNo);

    TError distrust;
    if (!committed) {
        distrust = TError("The consumer group has no committed offset");
    } else if (committed->Metadata != KafkaProgressMarkerMetadata) {
        distrust = TError("The committed offset metadata is not written by the sink, e.g. the offset was reset")
            .With("metadata", committed->Metadata);
    } else if (committed->Offset < maxPersistedSeqNo || committed->Offset > maxDistributedSeqNo) {
        distrust = TError("The committed offset is outside the seqNos the persisted state allows")
            .With("offset", committed->Offset);
    } else {
        return std::optional(committed->Offset);
    }

    // Without a marker, the earlier sessions' commits are known only to stop at the last message a persisted
    // epoch registered; if all of those are acknowledged, nothing is ambiguous.
    if (maxDistributedSeqNo <= maxPersistedSeqNo) {
        return std::optional<i64>();
    }
    return TError(
        "Kafka sink progress marker cannot be trusted, but messages the persisted state does not acknowledge may be "
        "committed to Kafka already; writing them again may duplicate them. The consumer group offset holding "
        "the marker expires after the broker's \"offsets.retention.minutes\" without commits. Set "
        "\"allow_missing_progress_marker\" in the dynamic sink parameters to write them anyway")
        .With("max_persisted_seq_no", maxPersistedSeqNo)
        .With("max_distributed_seq_no", maxDistributedSeqNo)
        .With(distrust);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

//! How the librdkafka transactional API asks callers to handle an error.
enum class ETransactionErrorKind
{
    None,
    //! The same call may be repeated.
    Retriable,
    //! The transaction must be aborted; the producer stays usable.
    Abortable,
    //! The writer cannot continue: the producer is unusable, e.g. a newer session fenced it, or the error is
    //! permanent.
    Fatal,
};

} // namespace

struct TTransactionalKafkaWriter::TCallResult
{
    TError Error;
    ETransactionErrorKind Kind = ETransactionErrorKind::None;
};

TTransactionalKafkaWriter::TCallResult TTransactionalKafkaWriter::ClassifyTransactionError(
    rd_kafka_error_t* error,
    TStringBuf operation)
{
    if (!error) {
        return {};
    }
    auto guard = Finally([&] {
        rd_kafka_error_destroy(error);
    });
    auto kind = ETransactionErrorKind::Fatal;
    if (rd_kafka_error_is_fatal(error)) {
        kind = ETransactionErrorKind::Fatal;
    } else if (rd_kafka_error_txn_requires_abort(error)) {
        kind = ETransactionErrorKind::Abortable;
    } else if (rd_kafka_error_is_retriable(error)) {
        kind = ETransactionErrorKind::Retriable;
    }
    return {
        .Error = TError("Kafka %v failed: %v", operation, rd_kafka_error_string(error)),
        .Kind = kind,
    };
}

////////////////////////////////////////////////////////////////////////////////

TTransactionalKafkaWriter::TTransactionalKafkaWriter(
    TKafkaClientPtr client,
    TTransactionalKafkaWriterOptions options,
    TKafkaTransactionalRecovery recovery,
    NLogging::TLogger logger,
    IStatusProfilerPtr statusProfiler)
    : Client_(std::move(client))
    , Options_(std::move(options))
    , Recovery_(recovery)
    , Logger(logger.WithTag("TransactionalId", Options_.TransactionalId))
    , ErrorState_(statusProfiler->ErrorState("writer"))
{ }

void TTransactionalKafkaWriter::Start()
{
    WriteQueue_ = New<TActionQueue>("KafkaTxnWrite");
    WriteQueue_->GetInvoker()->Invoke(BIND(&TTransactionalKafkaWriter::Run, MakeWeak(this)));
}

void TTransactionalKafkaWriter::Terminate()
{
    Terminated_.store(true);
    if (WriteQueue_) {
        WriteQueue_->Shutdown(/*graceful*/ true);
        WriteQueue_.Reset();
    }
}

TFuture<void> TTransactionalKafkaWriter::Write(TKafkaMessageToWrite&& message)
{
    return Queue_.Enqueue(std::move(message));
}

TError TTransactionalKafkaWriter::GetFatalError() const
{
    return Queue_.GetFatalError();
}

void TTransactionalKafkaWriter::Fail(const TError& error)
{
    YT_TLOG_ERROR("Kafka transactional writer failed")
        .With(error);
    ErrorState_->SetError(error);
    Queue_.Fail(error);
}

void TTransactionalKafkaWriter::Run()
{
    // Creating a producer needs no broker, so a failure is a configuration error that retrying cannot fix.
    std::unique_ptr<cppkafka::Producer> producer;
    try {
        auto configuration = Client_->MakeBaseConfiguration();
        configuration.set("transactional.id", Options_.TransactionalId);
        configuration.set("enable.idempotence", "true");
        configuration.set("transaction.timeout.ms", ToString(Options_.TransactionTimeout.MilliSeconds()));
        producer = std::make_unique<cppkafka::Producer>(std::move(configuration));
    } catch (const std::exception& ex) {
        Fail(TError("Failed to create transactional Kafka producer").With(ex));
        return;
    }

    auto markerOrError = Initialize(*producer);
    if (!markerOrError.IsOK()) {
        if (!Terminated_.load()) {
            Fail(TError("Kafka transactional writer failed to start").With(markerOrError));
        } else {
            Queue_.Fail(markerOrError);
        }
        return;
    }
    const auto marker = markerOrError.Value();

    std::unique_ptr<rd_kafka_consumer_group_metadata_t, decltype(&rd_kafka_consumer_group_metadata_destroy)> groupMetadata(
        rd_kafka_consumer_group_metadata_new(Options_.TransactionalId.c_str()),
        &rd_kafka_consumer_group_metadata_destroy);

    // The records taken from the queue; those before |nextIndex| are committed.
    std::vector<TKafkaMessageToWrite> backlog;
    size_t nextIndex = 0;
    i64 batchLimit = Options_.MaxTransactionRecordCount;
    int failureCount = 0;
    while (!Terminated_.load()) {
        if (nextIndex == backlog.size()) {
            backlog.clear();
            nextIndex = 0;
            i64 skippedCount = 0;
            for (auto& record : Queue_.TakePending(Options_.MaxTransactionRecordCount, Options_.MaxTransactionByteSize)) {
                // The replay numbers messages from the persisted frontier as the earlier sessions did, so the
                // records the marker covers are committed already.
                if (marker && record.SeqNo <= *marker) {
                    Queue_.Complete(record.SeqNo, TError());
                    ++skippedCount;
                } else {
                    backlog.push_back(std::move(record));
                }
            }
            if (skippedCount > 0) {
                YT_TLOG_INFO("Skipping replayed messages already committed to Kafka")
                    .With("Count", skippedCount)
                    .With("MarkerSeqNo", *marker);
            }
            if (backlog.empty()) {
                // Waits for more records.
                producer->poll(ProducerPollTimeout);
                continue;
            }
        }

        auto batch = std::span<const TKafkaMessageToWrite>(backlog).subspan(
            nextIndex,
            std::min<size_t>(batchLimit, backlog.size() - nextIndex));
        auto startTime = TInstant::Now();
        auto result = TryCommitTransaction(*producer, groupMetadata.get(), batch);
        if (Terminated_.load()) {
            break;
        }

        if (result.Kind == ETransactionErrorKind::None) {
            YT_TLOG_DEBUG("Kafka transaction committed")
                .With("RecordCount", batch.size())
                .With("MarkerSeqNo", batch.back().SeqNo)
                .With("Duration", TInstant::Now() - startTime);
            for (const auto& record : batch) {
                Queue_.Complete(record.SeqNo, TError());
            }
            nextIndex += batch.size();
            failureCount = 0;
            batchLimit = std::min(batchLimit * 2, Options_.MaxTransactionRecordCount);
            ErrorState_->ClearError();
            continue;
        }

        if (result.Kind == ETransactionErrorKind::Fatal || ++failureCount >= Options_.MaxConsecutiveFailures) {
            Fail(TError("Kafka transaction failed").With("attempt_count", failureCount).With(result.Error));
            return;
        }
        // The aborted records stay invisible to read-committed consumers. A smaller batch fits the
        // transaction timeout better and isolates a record the broker rejects, which then exhausts the budget.
        batchLimit = std::max<i64>(1, std::ssize(batch) / 2);
        YT_TLOG_WARNING("Kafka transaction aborted; writing its records again")
            .With("RecordCount", batch.size())
            .With("NextRecordCount", batchLimit)
            .With("FailureCount", failureCount)
            .With(result.Error);
        ErrorState_->SetError(result.Error);
        SleepUnlessTerminated(Terminated_, Options_.RetryBackoff);
    }

    // A transaction left open would hold read-committed consumers back until the broker times it out.
    // Aborting when none is open fails harmlessly.
    if (auto* error = rd_kafka_abort_transaction(producer->get_handle(), ToTimeoutMs(TransactionAbortOnTerminateTimeout))) {
        rd_kafka_error_destroy(error);
    }
    // Whatever is left uncommitted is replayed after the restart.
    Queue_.Fail(TError("Kafka writer terminated before the message was committed"));
}

TErrorOr<std::optional<i64>> TTransactionalKafkaWriter::Initialize(cppkafka::Producer& producer)
{
    // Fences the producers of earlier sessions with this transactional id and completes a transaction one
    // of them left open, so the marker read next is final.
    auto initResult = CallRetrying(
        [&] {
            return rd_kafka_init_transactions(producer.get_handle(), ToTimeoutMs(TransactionRequestTimeout));
        },
        "transaction initialization");
    if (initResult.Kind != ETransactionErrorKind::None) {
        return initResult.Error;
    }

    std::optional<TKafkaCommittedOffset> committed;
    for (int attempt = 1;; ++attempt) {
        auto committedOrError = ReadCommittedOffset();
        if (committedOrError.IsOK()) {
            committed = std::move(committedOrError.Value());
            break;
        }
        if (attempt >= Options_.MaxConsecutiveFailures || Terminated_.load()) {
            return TError("Failed to read the Kafka sink progress marker")
                .With("attempt_count", attempt)
                .With(committedOrError);
        }
        YT_TLOG_WARNING("Failed to read the Kafka sink progress marker, retrying")
            .With(committedOrError);
        ErrorState_->SetError(committedOrError);
        SleepUnlessTerminated(Terminated_, Options_.RetryBackoff);
    }

    std::optional<i64> marker;
    auto markerOrError = ResolveKafkaProgressMarker(committed, Recovery_);
    if (markerOrError.IsOK()) {
        marker = markerOrError.Value();
    } else if (Options_.AllowMissingProgressMarker) {
        YT_TLOG_WARNING("Writing messages that may be committed to Kafka already, as allowed")
            .With(markerOrError);
    } else {
        return TError(markerOrError).With("group_id", Options_.TransactionalId);
    }

    YT_TLOG_INFO("Kafka transactional writer initialized")
        .With("MarkerSeqNo", marker.value_or(-1))
        .With("MaxPersistedSeqNo", Recovery_.MaxPersistedSeqNo)
        .With("MaxDistributedSeqNo", Recovery_.MaxDistributedSeqNo);
    ErrorState_->ClearError();
    return marker;
}

TErrorOr<std::optional<TKafkaCommittedOffset>> TTransactionalKafkaWriter::ReadCommittedOffset()
{
    try {
        auto configuration = Client_->MakeBaseConfiguration();
        configuration.set("group.id", Options_.TransactionalId);
        configuration.set("enable.auto.commit", "false");
        // Only the offsets of committed transactions count.
        configuration.set("isolation.level", "read_committed");
        cppkafka::Consumer consumer(std::move(configuration));

        auto* partitions = rd_kafka_topic_partition_list_new(/*size*/ 1);
        auto partitionsGuard = Finally([&] {
            rd_kafka_topic_partition_list_destroy(partitions);
        });
        rd_kafka_topic_partition_list_add(partitions, Options_.Topic.c_str(), ProgressMarkerPartition);
        auto error = rd_kafka_committed(consumer.get_handle(), partitions, ToTimeoutMs(TransactionRequestTimeout));
        if (error == RD_KAFKA_RESP_ERR_NO_ERROR) {
            error = partitions->elems[0].err;
        }
        if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
            return TError("Failed to read the committed offset of consumer group %Qv: %v",
                Options_.TransactionalId,
                rd_kafka_err2str(error))
                .With("sasl_username", Client_->GetSaslUsername());
        }

        const auto& partition = partitions->elems[0];
        if (partition.offset < 0) {
            return std::optional<TKafkaCommittedOffset>();
        }
        return std::optional(TKafkaCommittedOffset{
            .Offset = partition.offset,
            .Metadata = std::string(static_cast<const char*>(partition.metadata), partition.metadata_size),
        });
    } catch (const std::exception& ex) {
        return TError(ex);
    }
}

TTransactionalKafkaWriter::TCallResult TTransactionalKafkaWriter::CallRetrying(
    const std::function<rd_kafka_error_t*()>& call,
    TStringBuf operation)
{
    for (int attempt = 1;; ++attempt) {
        auto result = ClassifyTransactionError(call(), operation);
        if (result.Kind != ETransactionErrorKind::Retriable) {
            return result;
        }
        // Giving up is safe even when the outcome of a commit is open: the restart learns it from the marker.
        if (attempt >= Options_.MaxConsecutiveFailures || Terminated_.load()) {
            return {
                .Error = TError("Kafka %v failed repeatedly", operation)
                    .With("attempt_count", attempt)
                    .With(result.Error),
                .Kind = ETransactionErrorKind::Fatal,
            };
        }
        YT_TLOG_WARNING("Kafka request failed, retrying")
            .With("Operation", operation)
            .With(result.Error);
        SleepUnlessTerminated(Terminated_, Options_.RetryBackoff);
    }
}

TTransactionalKafkaWriter::TCallResult TTransactionalKafkaWriter::TryCommitTransaction(
    cppkafka::Producer& producer,
    const rd_kafka_consumer_group_metadata_s* groupMetadata,
    std::span<const TKafkaMessageToWrite> records)
{
    auto* handle = producer.get_handle();
    auto timeoutMs = ToTimeoutMs(TransactionRequestTimeout);

    auto abort = [&] (TError cause) -> TCallResult {
        auto abortResult = CallRetrying(
            [&] {
                return rd_kafka_abort_transaction(handle, timeoutMs);
            },
            "transaction abort");
        if (abortResult.Kind != ETransactionErrorKind::None) {
            return {
                .Error = TError("Failed to abort Kafka transaction").With(abortResult.Error).With(cause),
                .Kind = ETransactionErrorKind::Fatal,
            };
        }
        return {
            .Error = std::move(cause),
            .Kind = ETransactionErrorKind::Abortable,
        };
    };
    auto finish = [&] (TCallResult result) -> TCallResult {
        return result.Kind == ETransactionErrorKind::Abortable ? abort(std::move(result.Error)) : result;
    };

    // Beginning is local; it fails only in a state the writer cannot leave.
    if (auto result = ClassifyTransactionError(rd_kafka_begin_transaction(handle), "transaction begin");
        result.Kind != ETransactionErrorKind::None)
    {
        result.Kind = ETransactionErrorKind::Fatal;
        return result;
    }

    for (const auto& record : records) {
        try {
            ProduceKafkaMessage(producer, MakeKafkaMessageBuilder(Options_.Topic, record, Options_.MessageIdHeader));
        } catch (const cppkafka::HandleException& ex) {
            auto error = TError("Failed to produce Kafka message").With("seq_no", record.SeqNo).With(ex);
            std::array<char, 512> fatalErrorString{};
            if (IsPermanentProduceError(ex.get_error().get_error()) ||
                rd_kafka_fatal_error(handle, fatalErrorString.data(), fatalErrorString.size()) != RD_KAFKA_RESP_ERR_NO_ERROR)
            {
                return {.Error = std::move(error), .Kind = ETransactionErrorKind::Fatal};
            }
            return abort(std::move(error));
        } catch (const std::exception& ex) {
            return {
                .Error = TError("Failed to produce Kafka message").With("seq_no", record.SeqNo).With(ex),
                .Kind = ETransactionErrorKind::Fatal,
            };
        }
    }

    // The marker commits with the records, so it covers exactly what is committed.
    std::string metadata(KafkaProgressMarkerMetadata);
    auto* offsets = rd_kafka_topic_partition_list_new(/*size*/ 1);
    auto* marker = rd_kafka_topic_partition_list_add(offsets, Options_.Topic.c_str(), ProgressMarkerPartition);
    marker->offset = records.back().SeqNo;
    // Borrowed from |metadata|; detached before the list frees its elements.
    marker->metadata = metadata.data();
    marker->metadata_size = metadata.size();
    auto offsetsGuard = Finally([&] {
        marker->metadata = nullptr;
        marker->metadata_size = 0;
        rd_kafka_topic_partition_list_destroy(offsets);
    });
    if (auto result = finish(CallRetrying(
        [&] {
            return rd_kafka_send_offsets_to_transaction(handle, offsets, groupMetadata, timeoutMs);
        },
        "progress marker commit"));
        result.Kind != ETransactionErrorKind::None)
    {
        return result;
    }

    // A retriable failure leaves the outcome open; committing again settles it.
    return finish(CallRetrying(
        [&] {
            return rd_kafka_commit_transaction(handle, timeoutMs);
        },
        "transaction commit"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
