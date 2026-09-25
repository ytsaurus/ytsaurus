#include "source.h"

#include "kafka_client.h"
#include "kafka_info.h"
#include "private.h"

#include <yt/yt/flow/library/cpp/connectors/common/source_controller_base.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

#include <library/cpp/yt/memory/shared_range.h>

#include <algorithm>
#include <limits>
#include <numeric>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

using NTableClient::FromUnversionedValue;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr int KafkaKeyPartitionIndexColumn = 1;
constexpr int KafkaKeyExpectedColumns = 2;

constexpr int KafkaDataColumnId = 0;
constexpr int KafkaKeyColumnId = 1;
constexpr int KafkaUnparsedColumnId = 2;
constexpr int KafkaPartitionColumnId = 3;
constexpr int KafkaOffsetColumnId = 4;
constexpr int KafkaSubOffsetColumnId = 5;
constexpr int KafkaTimestampColumnId = 6;

} // namespace

////////////////////////////////////////////////////////////////////////////////

TKey GenerateKafkaKey(std::string_view sourceIdentity, int partitionIndex)
{
    return MakeKey(TStringBuf(sourceIdentity), partitionIndex);
}

int ExtractKafkaPartitionIndex(const TKey& key)
{
    if (key.Underlying().GetCount() != KafkaKeyExpectedColumns) {
        THROW_ERROR_EXCEPTION("Kafka key should have exactly %v fields, got: %v",
            KafkaKeyExpectedColumns,
            key.Underlying().GetCount())
            .With("kafka_key", key);
    }
    return FromUnversionedValue<i64>(key.Underlying()[KafkaKeyPartitionIndexColumn]);
}

std::vector<int> SelectKafkaPartitions(
    int partitionCount,
    const std::optional<std::vector<std::pair<int, int>>>& filter)
{
    std::vector<int> selected;
    for (int index = 0; index < partitionCount; ++index) {
        if (filter) {
            bool keep = false;
            for (const auto& [begin, end] : *filter) {
                if (index >= begin && index < end) {
                    keep = true;
                    break;
                }
            }
            if (!keep) {
                continue;
            }
        }
        selected.push_back(index);
    }
    return selected;
}

NTableClient::TTableSchemaPtr GetKafkaSourceSchema()
{
    static const auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("data", EValueType::String),
        TColumnSchema("key", EValueType::String),
        TColumnSchema("unparsed", EValueType::Any),
        TColumnSchema("partition", EValueType::Int64),
        TColumnSchema("offset", EValueType::Int64),
        TColumnSchema("sub_offset", EValueType::Int64),
        TColumnSchema("timestamp", EValueType::Int64),
    });
    return schema;
}

////////////////////////////////////////////////////////////////////////////////

i64 GetKafkaRowOverhead()
{
    static const i64 Overhead = [] {
        const auto columnCount = GetKafkaSourceSchema()->GetColumnCount();
        auto row = TPayload::TUnderlying(
            columnCount,
            /*stringDataSize*/ 0,
            [columnCount] (TMutableUnversionedRow row) {
                for (int index = 0; index < columnCount; ++index) {
                    row[index] = MakeUnversionedNullValue(index);
                }
            });
        return static_cast<i64>(row.GetSpaceUsed()) + static_cast<i64>(sizeof(TPayload));
    }();
    return Overhead;
}

i64 GetKafkaExpandedSize(i64 rowCount, i64 outputStringBytes, i64 keyBytes)
{
    return rowCount * (keyBytes + GetKafkaRowOverhead()) + outputStringBytes;
}

i64 GetKafkaExpandedSize(const std::vector<TSharedRef>& frames, i64 keyBytes)
{
    auto outputStringBytes = std::accumulate(
        frames.begin(),
        frames.end(),
        i64{0},
        [] (i64 sum, const TSharedRef& frame) {
            return sum + static_cast<i64>(frame.Size());
        });
    return GetKafkaExpandedSize(std::ssize(frames), outputStringBytes, keyBytes);
}

////////////////////////////////////////////////////////////////////////////////

void TUnparsedKafkaPayload::Register(TRegistrar registrar)
{
    registrar.Parameter("data", &TThis::Data)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TKafkaSource::TKafkaSource(
    TSourceContextPtr context,
    TDynamicSourceContextPtr dynamicContext)
    : TIntegerOffsetOrderedSourceBase(std::move(context), std::move(dynamicContext))
    , Schema_(GetKafkaSourceSchema())
    , Topic_(GetParameters()->Topic)
    , PartitionIndex_(ExtractKafkaPartitionIndex(GetContext()->SourceKey))
    , Logger(TOrderedSourceBase::Logger
            .WithTag("Topic", Topic_)
            .WithTag("PartitionIndex", PartitionIndex_)
            .WithTag("GroupId", GetParameters()->GroupId))
    , Client_(GetContext()->GetStaticResource(KafkaClientDefaultResourceId)->As<TKafkaClient>())
    , MalformedMessagesCounter_(GetProfiler().Counter("/malformed_messages"))
{ }

void TKafkaSource::DoInit()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);

    // Not an availability voter: a failing partition info update says nothing about whether reads work.
    UpdatePartitionInfoErrorState_ = GetContext()->StatusProfiler->ErrorState("update_partition_info");
    ReadErrorState_ = CreateAvailabilityErrorState("/read");
    TopicIdentityErrorState_ = CreateAvailabilityErrorState("/topic_identity");
    ReadSession_ = New<TKafkaReadSession>(
        Client_,
        Topic_,
        PartitionIndex_,
        GetParameters()->GroupId,
        PersistedOffsetExclusive_,
        GetParameters()->UseConsumerGroupOffset,
        GetParameters()->MaxBufferBytes,
        GetDynamicParameters()->PollTimeout,
        GetDynamicParameters()->WatermarkUpdatePeriod,
        GetParameters()->MetadataTimeout,
        Logger,
        ReadErrorState_);
    ReadSession_->Start();

    SubscribeReconfigured(
        BIND([this] (const TDynamicSourceContextPtr& /*dynamicContext*/) {
            if (ReadSession_) {
                ReadSession_->Reconfigure(
                    GetDynamicParameters()->PollTimeout,
                    GetDynamicParameters()->WatermarkUpdatePeriod);
            }
        }).Via(GetContext()->SerializedInvoker));

    PartitionInfoUpdater_ = New<TPeriodicExecutor>(
        GetContext()->SerializedInvoker,
        BIND(&TKafkaSource::TryUpdatePartitionInfo, MakeWeak(this)),
        TPeriodicExecutorOptions::WithJitter(GetParameters()->UpdateInfoPeriod));
    PartitionInfoUpdater_->Start();
}

void TKafkaSource::DoTerminate()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);

    if (PartitionInfoUpdater_) {
        YT_UNUSED_FUTURE(PartitionInfoUpdater_->Stop());
    }
    if (ReadSession_) {
        ReadSession_->Terminate();
        ReadSession_ = {};
    }
    PendingMessages_.clear();
}

std::vector<TSharedRef> TKafkaSource::UnpackData(TSharedRef data) const
{
    return {std::move(data)};
}

i64 TKafkaSource::GetMaxFramesPerRecord() const
{
    return std::numeric_limits<i64>::max();
}

i64 TKafkaSource::GetMaxExpandedBytesPerRecord() const
{
    return std::numeric_limits<i64>::max();
}

void TKafkaSource::ValidateExpandedSize(
    i64 expandedSize,
    const TKafkaReadSession::TKafkaMessage& message) const
{
    auto maxExpandedSize = GetMaxExpandedBytesPerRecord();
    if (expandedSize <= maxExpandedSize) {
        return;
    }

    THROW_ERROR_EXCEPTION("Kafka record expands into %v bytes, above the limit of %v",
        expandedSize,
        maxExpandedSize)
        .With("topic", Topic_)
        .With("partition_index", PartitionIndex_)
        .With("offset", message.Offset);
}

std::pair<std::vector<TPayload>, TTableSchemaPtr> TKafkaSource::ProcessMessage(
    TKafkaReadSession::TKafkaMessage& message)
{
    const TStringBuf keyBuf = message.Key ? TStringBuf(*message.Key) : TStringBuf();
    const i64 keyBytes = message.Key ? std::ssize(*message.Key) : 0;

    if (!message.Value) {
        ValidateExpandedSize(GetKafkaExpandedSize(1, 0, keyBytes), message);

        // A compacted-topic tombstone: emit the row with a null value (distinct from an empty string)
        // so deletion semantics survive the pipeline.
        auto payload = TPayload(TPayload::TUnderlying(
            Schema_->GetColumnCount(),
            keyBytes,
            [&] (TMutableUnversionedRow row) {
                row[KafkaDataColumnId] = MakeUnversionedNullValue(KafkaDataColumnId);
                row[KafkaKeyColumnId] = message.Key
                    ? MakeUnversionedStringValue(keyBuf, KafkaKeyColumnId)
                    : MakeUnversionedNullValue(KafkaKeyColumnId);
                row[KafkaUnparsedColumnId] = MakeUnversionedNullValue(KafkaUnparsedColumnId);
                row[KafkaPartitionColumnId] = MakeUnversionedInt64Value(PartitionIndex_, KafkaPartitionColumnId);
                row[KafkaOffsetColumnId] = MakeUnversionedInt64Value(message.Offset, KafkaOffsetColumnId);
                row[KafkaSubOffsetColumnId] = MakeUnversionedInt64Value(0, KafkaSubOffsetColumnId);
                row[KafkaTimestampColumnId] = MakeUnversionedInt64Value(message.TimestampSeconds, KafkaTimestampColumnId);
            }));
        return {{std::move(payload)}, Schema_};
    }

    // The caller discards |message| after this call, so move its payload into the holder instead of
    // copying every record's bytes on the read hot path.
    auto dataPtr = std::make_shared<std::string>(std::move(*message.Value));

    std::vector<TSharedRef> unpacked;
    try {
        unpacked = UnpackData(TSharedRef(TRef::FromString(*dataPtr), MakeSharedRangeHolder(dataPtr)));
    } catch (const std::exception& ex) {
        return HandleMalformedMessage(message, *dataPtr, keyBuf, keyBytes, ex);
    }

    // Outside the catch: an oversized record is well formed, so the malformed-message policy must
    // not drop it.
    if (auto maxFrames = GetMaxFramesPerRecord(); std::ssize(unpacked) > maxFrames) {
        THROW_ERROR_EXCEPTION("Kafka record unpacks into at least %v messages, above the limit of %v",
            unpacked.size(),
            maxFrames)
            .With("topic", Topic_)
            .With("partition_index", PartitionIndex_)
            .With("offset", message.Offset);
    }
    ValidateExpandedSize(GetKafkaExpandedSize(unpacked, keyBytes), message);

    std::vector<TPayload> payloads;
    payloads.reserve(unpacked.size());
    for (int i = 0; i < std::ssize(unpacked); ++i) {
        payloads.push_back(TPayload(TPayload::TUnderlying(
            Schema_->GetColumnCount(),
            unpacked[i].Size() + keyBytes,
            [&] (TMutableUnversionedRow row) {
                row[KafkaDataColumnId] = MakeUnversionedStringValue(unpacked[i].ToStringBuf(), KafkaDataColumnId);
                row[KafkaKeyColumnId] = message.Key
                    ? MakeUnversionedStringValue(keyBuf, KafkaKeyColumnId)
                    : MakeUnversionedNullValue(KafkaKeyColumnId);
                row[KafkaUnparsedColumnId] = MakeUnversionedNullValue(KafkaUnparsedColumnId);
                row[KafkaPartitionColumnId] = MakeUnversionedInt64Value(PartitionIndex_, KafkaPartitionColumnId);
                row[KafkaOffsetColumnId] = MakeUnversionedInt64Value(message.Offset, KafkaOffsetColumnId);
                row[KafkaSubOffsetColumnId] = MakeUnversionedInt64Value(i, KafkaSubOffsetColumnId);
                row[KafkaTimestampColumnId] = MakeUnversionedInt64Value(message.TimestampSeconds, KafkaTimestampColumnId);
            })));
    }
    return {std::move(payloads), Schema_};
}

std::pair<std::vector<TPayload>, TTableSchemaPtr> TKafkaSource::HandleMalformedMessage(
    const TKafkaReadSession::TKafkaMessage& message,
    const std::string& data,
    TStringBuf keyBuf,
    i64 keyBytes,
    const std::exception& ex)
{
    YT_TLOG_WARNING("Failed to process Kafka message")
        .With("Offset", message.Offset)
        .With(ex);
    MalformedMessagesCounter_.Increment();
    switch (GetDynamicParameters()->MalformedMessagePolicy) {
        case EMalformedKafkaMessagePolicy::Keep: {
            auto unparsed = New<TUnparsedKafkaPayload>();
            unparsed->Data = data;
            const auto unparsedYson = ConvertToYsonString(unparsed);
            ValidateExpandedSize(
                GetKafkaExpandedSize(1, static_cast<i64>(unparsedYson.AsStringBuf().size()), keyBytes),
                message);
            auto payload = TPayload(TPayload::TUnderlying(
                Schema_->GetColumnCount(),
                unparsedYson.AsStringBuf().size() + keyBytes,
                [&] (TMutableUnversionedRow row) {
                    row[KafkaDataColumnId] = MakeUnversionedNullValue(KafkaDataColumnId);
                    row[KafkaKeyColumnId] = message.Key
                        ? MakeUnversionedStringValue(keyBuf, KafkaKeyColumnId)
                        : MakeUnversionedNullValue(KafkaKeyColumnId);
                    row[KafkaUnparsedColumnId] = MakeUnversionedAnyValue(unparsedYson.AsStringBuf(), KafkaUnparsedColumnId);
                    row[KafkaPartitionColumnId] = MakeUnversionedInt64Value(PartitionIndex_, KafkaPartitionColumnId);
                    row[KafkaOffsetColumnId] = MakeUnversionedInt64Value(message.Offset, KafkaOffsetColumnId);
                    row[KafkaSubOffsetColumnId] = MakeUnversionedInt64Value(0, KafkaSubOffsetColumnId);
                    row[KafkaTimestampColumnId] = MakeUnversionedInt64Value(message.TimestampSeconds, KafkaTimestampColumnId);
                }));
            return {{std::move(payload)}, Schema_};
        }
        case EMalformedKafkaMessagePolicy::Drop:
            break;
        case EMalformedKafkaMessagePolicy::Fail:
            THROW_ERROR_EXCEPTION("Failed to process Kafka message")
                .With("topic", Topic_)
                .With("partition_index", PartitionIndex_)
                .With("offset", message.Offset)
                .With(ex);
    }
    return {{}, Schema_};
}

TFuture<std::vector<TKafkaSource::TRecord>> TKafkaSource::DoReadNextBatch(
    const TMessageBatcherSettingsPtr& settings,
    TOffset nextOffsetAsKey,
    std::optional<TOffset> offsetLimitExclusiveAsKey)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);

    i64 nextOffset = OffsetToInt(nextOffsetAsKey);
    std::optional<i64> offsetLimitExclusive = offsetLimitExclusiveAsKey.transform(OffsetToInt);

    if (!TopicIdentityError_.IsOK()) {
        // Hard stop, not just an availability vote — see the field's comment.
        return MakeFuture<std::vector<TRecord>>(TopicIdentityError_);
    }

    // Adopted from the read path, not the periodic update: the sooner the base persists the new offset,
    // the shorter the window in which a restart has to reposition again.
    TryReposition();

    auto startOffset = ReadSession_->GetStartOffset();
    if (!startOffset || nextOffset < *startOffset) {
        // Until the session has settled its start offset and the base has caught up with it (via a
        // partition info update), |nextOffset| would read as a seek back below the start.
        return TDelayedExecutor::MakeDelayed(settings->BatchDuration)
            .Apply(BIND([] {
                return std::vector<TRecord>{};
            }));
    }

    try {
        TryAdvancePendingMessages(nextOffset);
        if (PendingMessages_.empty()) {
            auto messages = ReadSession_->GetBatch(
                nextOffset,
                offsetLimitExclusive,
                settings->MaxRowsPerBatch,
                settings->MaxBytesPerBatch);
            PendingMessages_.assign(
                std::make_move_iterator(messages.begin()),
                std::make_move_iterator(messages.end()));
        }

        std::vector<TRecord> records;
        records.reserve(PendingMessages_.size());
        i64 batchRows = 0;
        i64 batchBytes = 0;
        while (!PendingMessages_.empty()) {
            if (offsetLimitExclusive && PendingMessages_.front().Offset >= *offsetLimitExclusive) {
                // Held messages may predate the limit of a draining partition.
                break;
            }
            auto message = std::move(PendingMessages_.front());
            PendingMessages_.pop_front();

            auto [payloads, payloadSchema] = ProcessMessage(message);
            batchRows += std::ssize(payloads);
            for (const auto& payload : payloads) {
                batchBytes += static_cast<i64>(payload.Underlying().GetSpaceUsed());
            }
            records.push_back(TRecord{
                .Offset = IntToOffset(message.Offset),
                .WriteTimestamp = message.WriteTimestamp,
                .CreateTimestamp = message.CreateTimestamp,
                .Meta = std::nullopt,
                .Payloads = std::move(payloads),
                .PayloadSchema = payloadSchema,
            });
            if (batchRows >= settings->MaxRowsPerBatch || batchBytes >= settings->MaxBytesPerBatch) {
                // The fetch limits count records, not the rows they unpack into.
                break;
            }
        }

        if (records.empty()) {
            // Nothing buffered yet; back off so the base does not spin.
            return TDelayedExecutor::MakeDelayed(settings->BatchDuration)
                .Apply(BIND([] {
                    return std::vector<TRecord>{};
                }));
        }
        return MakeFuture(std::move(records));
    } catch (const std::exception& ex) {
        auto error = TError("Critical Kafka read failure").With(ex);
        YT_TLOG_ERROR("Critical Kafka read failure")
            .With(ex);
        return MakeFuture<std::vector<TRecord>>(error);
    }
}

void TKafkaSource::DoReportPersistedOffset(TOffset offset)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);

    PersistedOffsetExclusive_ = OffsetToInt(offset);
    if (ReadSession_) {
        ReadSession_->ReportPersistedOffset(PersistedOffsetExclusive_);
    }
}

void TKafkaSource::TryAdvancePendingMessages(i64 nextOffset)
{
    while (!PendingMessages_.empty() && PendingMessages_.front().Offset < nextOffset) {
        PendingMessages_.pop_front();
    }
}

void TKafkaSource::TryReposition()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);

    auto startOffset = ReadSession_->GetStartOffset();
    if (!startOffset || *startOffset <= PersistedOffsetExclusive_) {
        return;
    }

    PendingMessages_.clear();

    YT_TLOG_INFO("Repositioning Kafka partition to the consumer group offset")
        .With("StartOffset", *startOffset)
        .With("PersistedOffsetExclusive", PersistedOffsetExclusive_);

    PersistedOffsetExclusive_ = *startOffset;
    UpdatePartitionInfo({
        .CommittedOffsetExclusive = IntToOffset(*startOffset),
        .UpdateInstant = TInstant::Now(),
        .Repositioned = true,
    });
}

void TKafkaSource::TryUpdatePartitionInfo()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);

    try {
        if (ReadSession_) {
            if (auto watermarks = ReadSession_->GetWatermarks()) {
                auto [low, high, consumedOffsetExclusiveAtQuery] = *watermarks;
                // Compare against the offset sampled with the query, never the current one: on a
                // live topic the source keeps consuming and committing between two watermark queries,
                // so the current offset routinely runs ahead of the last sampled watermark by the
                // records written meanwhile. Only the sampled pair proves records went missing.
                if (high < consumedOffsetExclusiveAtQuery) {
                    // Fewer records on the broker than the pipeline had already taken: the topic was
                    // recreated (or truncated) under the same name. Sticky availability error plus a
                    // hard read stop; recovery requires resetting the pipeline state.
                    auto error = TError("Kafka high watermark is below the persisted offset; was the topic recreated?")
                        .With("high_watermark", high)
                        .With("consumed_offset_exclusive", consumedOffsetExclusiveAtQuery);
                    YT_TLOG_ERROR("Kafka topic appears recreated under the same name")
                        .With("HighWatermark", high)
                        .With("ConsumedOffsetExclusive", consumedOffsetExclusiveAtQuery);
                    TopicIdentityErrorState_->SetError(error);
                    TopicIdentityError_ = error;
                    return;
                }
                // The committed frontier drives Empty/Suspended, so report the persisted offset, which
                // the base already bounds from above; |low| dominates only after a retention trim,
                // firing the base's rewind. Clamping to the sampled |high| would pin the frontier at
                // an aged watermark and report a backlog the pipeline has long since drained.
                UpdatePartitionInfo({
                    .CommittedOffsetExclusive = IntToOffset(std::max(PersistedOffsetExclusive_, low)),
                    .MaxOffsetExclusive = IntToOffset(high),
                    .UpdateInstant = TInstant::Now(),
                });
            }
        }
        UpdatePartitionInfoErrorState_->ClearError();
    } catch (const std::exception& ex) {
        UpdatePartitionInfoErrorState_->SetError(TError("Failed to update partition info").With(ex));
        YT_TLOG_ERROR("Failed to update Kafka partition info")
            .With(ex);
    }
}

////////////////////////////////////////////////////////////////////////////////

TKafkaSourceController::TKafkaSourceController(
    TSourceControllerContextPtr context,
    TDynamicSourceControllerContextPtr dynamicContext)
    : TSourceControllerBase(std::move(context), std::move(dynamicContext))
    , BootstrapServers_(
        GetContext()->GetStaticResource(KafkaClientDefaultResourceId)->As<TKafkaClient>()->GetBootstrapServers())
    , Info_(New<TKafkaInfoController>(
        GetParameters(),
        GetContext()->GetStaticResource(KafkaClientDefaultResourceId)->As<TKafkaClient>(),
        GetContext()->Invoker,
        Logger,
        GetContext()->StatusProfiler->WithPrefix("/kafka_info_controller")))
{ }

void TKafkaSourceController::Init(IInitContextPtr initContext)
{
    Info_->Init(initContext->WithPrefix("kafka_info"));
}

void TKafkaSourceController::Sync()
{
    Info_->Sync();
}

void TKafkaSourceController::Commit()
{
    Info_->Commit();
}

std::optional<THashMap<TKey, IMapNodePtr>> TKafkaSourceController::ListKeys()
{
    auto count = Info_->GetPartitionCount();
    if (!count) {
        return std::nullopt;
    }

    const auto& parameters = GetParameters();
    const auto sourceIdentity = GetSourceIdentity();
    auto selected = SelectKafkaPartitions(*count, parameters->PartitionFilter);

    auto trivialSpec = GetEphemeralNodeFactory()->CreateMap();
    THashMap<TKey, IMapNodePtr> keys;
    for (int index : selected) {
        keys[GenerateKafkaKey(sourceIdentity, index)] = trivialSpec;
    }
    if (auto skipped = *count - std::ssize(selected); skipped != 0) {
        YT_TLOG_DEBUG("Skipped some partitions due to filter")
            .With("Skipped", skipped)
            .With("Left", std::size(keys));
    }
    return keys;
}

std::string TKafkaSourceController::GetSourceIdentity() const
{
    return MakeSourceIdentity({BootstrapServers_, GetParameters()->Topic});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
