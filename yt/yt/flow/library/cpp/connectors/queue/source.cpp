#include "source.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/schema.h>

#include <yt/yt/flow/library/cpp/connectors/common/flow_queue_meta.h>
#include <yt/yt/flow/library/cpp/connectors/common/sync_replica.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/queue_client/consumer_client.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/options.h>
#include <yt/yt/client/api/queue_client.h>
#include <yt/yt/client/api/queue_transaction.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/api/transaction_client.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NApi;

using NTableClient::FromUnversionedValue;

////////////////////////////////////////////////////////////////////////////////

TKey GenerateQueueKey(
    std::string_view sourceIdentity,
    int partitionIndex)
{
    return MakeKey(TStringBuf(sourceIdentity), partitionIndex);
}

namespace {

constexpr int QueueKeyPartitionIndexColumn = 1;
constexpr int QueueKeyExpectedColumns = 2;

} // namespace

int ExtractQueuePartitionIndex(const TKey& key)
{
    if (key.Underlying().GetCount() != QueueKeyExpectedColumns) {
        THROW_ERROR_EXCEPTION("Queue Key should have exactly %v fields, got: %v",
            QueueKeyExpectedColumns,
            key.Underlying().GetCount())
            .With("queue_key", key);
    }
    return FromUnversionedValue<i64>(key.Underlying()[QueueKeyPartitionIndexColumn]);
}

////////////////////////////////////////////////////////////////////////////////

std::pair<std::vector<TPayload>, NTableClient::TTableSchemaPtr> TQueueSource::UnpackRow(const NTableClient::TUnversionedRow& row, const NTableClient::TTableSchemaPtr& schema)
{
    return std::pair{std::vector{TPayload(TPayload::TUnderlying(row))}, schema};
}

////////////////////////////////////////////////////////////////////////////////

TQueueSourceController::TQueueSourceController(
    TSourceControllerContextPtr context,
    TDynamicSourceControllerContextPtr dynamicContext)
    : TSourceControllerBase(std::move(context), std::move(dynamicContext))
    , Info_(New<TQueueInfoController>(
        GetParameters(),
        GetContext()->ClientsCache->GetClient(*GetParameters()->QueuePath.GetCluster()),
        GetContext()->Invoker,
        GetContext()->Logger.WithTag("QueuePath", GetParameters()->QueuePath),
        GetContext()->StatusProfiler->WithPrefix("/queue_info")))
{
}

void TQueueSourceController::Init(IInitContextPtr initContext)
{
    Info_->Init(initContext->WithPrefix("queue_info"));
}

void TQueueSourceController::Sync()
{
    Info_->Sync();
}

void TQueueSourceController::Commit()
{
    Info_->Commit();
}

std::optional<THashMap<TKey, NYTree::IMapNodePtr>> TQueueSourceController::ListKeys()
{
    auto count = Info_->GetPartitionCount();
    if (!count) {
        return std::nullopt;
    }
    const auto& parameters = GetParameters();
    const auto& filter = parameters->PartitionFilter;
    const auto sourceIdentity = GetSourceIdentity();

    auto trivialSpec = NYTree::GetEphemeralNodeFactory()->CreateMap();
    THashMap<TKey, NYTree::IMapNodePtr> keys;
    i64 skipped = 0;
    for (int i = 0; i < *count; ++i) {
        if (filter) {
            bool keep = false;
            for (const auto& [begin, end] : *filter) {
                if (i >= begin && i < end) {
                    keep = true;
                    break;
                }
            }
            if (!keep) {
                ++skipped;
                continue;
            }
        }
        keys[GenerateQueueKey(sourceIdentity, i)] = trivialSpec;
    }
    if (skipped != 0) {
        YT_TLOG_DEBUG("Skipped some partitions due to filter")
            .With("Skipped", skipped)
            .With("Left", std::size(keys));
    }
    return keys;
}

std::string TQueueSourceController::GetSourceIdentity() const
{
    const auto& queuePath = GetParameters()->QueuePath;
    return MakeSourceIdentity({queuePath.GetCluster().value_or(""), TStringBuf(queuePath.GetPath())});
}

////////////////////////////////////////////////////////////////////////////////

TQueueSourceImpl::TQueueSourceImpl(
    TSourceContextPtr context,
    TDynamicSourceContextPtr dynamicContext)
    : TIntegerOffsetOrderedSourceBase(std::move(context), std::move(dynamicContext))
    , PartitionIndex_(ExtractQueuePartitionIndex(GetContext()->SourceKey))
    , Logger(TOrderedSourceBase::Logger
            .WithTag("QueuePath", GetParameters()->QueuePath)
            .WithTag("ConsumerPath", GetParameters()->ConsumerPath)
            .WithTag("PartitionIndex", PartitionIndex_))
    , ConsumerClient_(GetContext()->ClientsCache->GetClient(*GetParameters()->ConsumerPath.GetCluster()))
    , QueueClient_(GetContext()->ClientsCache->GetClient(*GetParameters()->QueuePath.GetCluster()))
    , SubConsumerClient_(NQueueClient::CreateSubConsumerClient(
        ConsumerClient_,
        QueueClient_,
        GetParameters()->ConsumerPath.GetPath(),
        GetParameters()->QueuePath.GetPath()))
    , UpdatePartitionInfoErrorState_(GetContext()->StatusProfiler->ErrorState("/update_partition_info"))
    , InitialCommittedOffset_(NewPromise<i64>())
{ }

void TQueueSourceImpl::DoInit()
{
    ReadErrorState_ = CreateAvailabilityErrorState("/read");

    PartitionInfoUpdater_ = New<NConcurrency::TPeriodicExecutor>(
        GetContext()->SerializedInvoker,
        BIND(&TQueueSourceImpl::TryUpdatePartitionInfo, MakeWeak(this)),
        NConcurrency::TPeriodicExecutorOptions::WithJitter(this->GetParameters()->UpdateInfoPeriod));
    PartitionInfoUpdater_->Start();
    PartitionInfoUpdater_->ScheduleOutOfBand(); // Committed offset is required to perform first read batch operation.
}

void TQueueSourceImpl::DoTerminate()
{
    if (PartitionInfoUpdater_) {
        YT_UNUSED_FUTURE(PartitionInfoUpdater_->Stop());
    }
    Terminated_ = true;
    if (ReadAhead_.PullFuture) {
        ReadAhead_.PullFuture.Cancel(TError("Source is terminated, so result of current request is useless"));
        ReadAhead_.PullFuture = {};
    }
    InitialCommittedOffset_.TrySet(TError(NYT::EErrorCode::Canceled, "Source is terminated"));
}

NObjectClient::EObjectType TQueueSourceImpl::GetQueueObjectType()
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);

    if (QueueObjectType_) {
        return *QueueObjectType_;
    }

    TGetNodeOptions options;
    options.Attributes = {"type"};
    options.ReadFrom = NApi::EMasterChannelKind::Cache;
    auto ysonString = NConcurrency::WaitFor(QueueClient_->GetNode(GetParameters()->QueuePath.GetPath(), options))
        .ValueOrThrow();
    QueueObjectType_ = NYTree::ConvertToNode(ysonString)->Attributes().Get<NObjectClient::EObjectType>("type");
    return *QueueObjectType_;
}

bool TQueueSourceImpl::IsReplicatedTableQueue()
{
    return GetQueueObjectType() == NObjectClient::EObjectType::ReplicatedTable;
}

TQueueSourceImpl::TSyncReplica TQueueSourceImpl::ResolveSyncReplica()
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);

    const auto& queuePath = GetParameters()->QueuePath.GetPath();

    TGetNodeOptions options;
    options.ReadFrom = NApi::EMasterChannelKind::Cache;
    auto replicasYson = NConcurrency::WaitFor(QueueClient_->GetNode(queuePath + "/@replicas", options))
        .ValueOrThrow();
    auto location = FindEnabledSyncReplica(NYTree::ConvertToNode(replicasYson)->AsMap(), queuePath);
    return TSyncReplica{
        .Client = GetContext()->ClientsCache->GetClient(location.ClusterName),
        .Path = std::move(location.Path),
    };
}

TQueueSourceImpl::TTrimInfo TQueueSourceImpl::FetchTrimInfo()
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);

    // A plain replicated table's meta object can report a trim point ahead of all its replicas, so read the trim from
    // a synchronous replica instead. Plain ordered and chaos replicated tables report a correct trim themselves.
    auto client = QueueClient_;
    auto path = GetParameters()->QueuePath.GetPath();
    if (IsReplicatedTableQueue()) {
        auto replica = ResolveSyncReplica();
        client = std::move(replica.Client);
        path = std::move(replica.Path);
    }

    auto tabletInfos = NConcurrency::WaitFor(client->GetTabletInfos(path, std::vector<int>{PartitionIndex_}))
        .ValueOrThrow();
    YT_VERIFY(tabletInfos.size() == 1);
    return TTrimInfo{
        .TrimmedRowCount = tabletInfos[0].TrimmedRowCount,
        .TotalRowCount = tabletInfos[0].TotalRowCount,
    };
}

void TQueueSourceImpl::TryUpdatePartitionInfo()
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);

    try {
        const auto startInstant = TInstant::Now();

        auto consumerInfoFuture = SubConsumerClient_->CollectPartitions(std::vector<int>{PartitionIndex_});
        const auto trimInfo = FetchTrimInfo();
        const auto consumerInfo = NConcurrency::WaitFor(consumerInfoFuture).ValueOrThrow();

        YT_VERIFY(consumerInfo.size() == 1);
        YT_VERIFY(consumerInfo[0].PartitionIndex == PartitionIndex_);

        const i64 committedOffset = consumerInfo[0].NextRowIndex;
        InitialCommittedOffset_.TrySet(committedOffset);

        // The trim point is a valid read lower bound: it keeps lag falling on trims while stopped and avoids reading
        // already-trimmed offsets.
        CommittedOffsetExclusive_.store(std::max(committedOffset, trimInfo.TrimmedRowCount));
        MaxOffsetExclusive_.store(trimInfo.TotalRowCount);

        UpdatePartitionInfo(
            TPartitionInfoUpdate{
                .CommittedOffsetExclusive = IntToOffset(CommittedOffsetExclusive_.load()),
                .MaxOffsetExclusive = IntToOffset(MaxOffsetExclusive_.load()),
                .UpdateInstant = startInstant,
            });

        if (PersistedOffsetExclusive_.load() > committedOffset) {
            auto transaction = NConcurrency::WaitFor(ConsumerClient_->StartTransaction(NTransactionClient::ETransactionType::Tablet)).ValueOrThrow();
            SubConsumerClient_->Advance(transaction, PartitionIndex_, committedOffset, PersistedOffsetExclusive_.load());
            NConcurrency::WaitFor(transaction->Commit()).ThrowOnError();
        }
        UpdatePartitionInfoErrorState_->ClearError();
    } catch (const std::exception& ex) {
        auto error = TError("Failed to update partition info").With(ex);
        UpdatePartitionInfoErrorState_->SetError(error);
    }
}

void TQueueSourceImpl::DoReportPersistedOffset(TOffset offsetExclusive)
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);

    PersistedOffsetExclusive_.store(OffsetToInt(offsetExclusive));

    // Heuristic for fast committing last offset in case of finite source. Excess TryUpdatePartitionInfo call is safe.
    if (GetParameters()->Finite &&
        CommittedOffsetExclusive_.load() < PersistedOffsetExclusive_.load() &&
        PersistedOffsetExclusive_.load() == MaxOffsetExclusive_.load())
    {
        PartitionInfoUpdater_->ScheduleOutOfBand();
    }
}

void TQueueSourceImpl::ResetReadAhead(i64 nextOffset)
{
    if (ReadAhead_.PullFuture) {
        ReadAhead_.PullFuture.Cancel(TError("Read offset has changed, so result of current request is useless"));
    }
    ReadAhead_ = {
        .PullOffset = nextOffset,
        .ReadOffset = nextOffset,
    };
}

void TQueueSourceImpl::StartPull(const TMessageBatcherSettingsPtr& settings)
{
    YT_VERIFY(!ReadAhead_.PullFuture);

    NQueueClient::TQueueRowBatchReadOptions rowBatchReadOptions;
    rowBatchReadOptions.MaxRowCount = settings->MaxRowsPerBatch;
    rowBatchReadOptions.MaxDataWeight = settings->MaxBytesPerBatch;

    ReadAhead_.PullFuture = InitialCommittedOffset_
        .ToFuture()
        .ToImmediatelyCancelable(/*propagateCancelation*/ false)
        .Apply(BIND([this, this_ = MakeStrong(this), settings, pullOffset = ReadAhead_.PullOffset, rowBatchReadOptions] (const i64& initialOffset) {
            NApi::TPullQueueConsumerOptions options;
            options.Timeout = GetDynamicParameters()->PullQueueTimeout;
            // A no-op for a plain ordered table; for a replicated/chaos queue it forces reading a synchronous replica.
            options.ReplicaConsistency = NApi::EReplicaConsistency::Sync;

            // Optimistically start reading from pullOffset (drop messages with offset less than initial later).
            // TODO(mikari): consumer or queue client?
            return ConsumerClient_->PullQueueConsumer(
                GetParameters()->ConsumerPath,
                GetParameters()->QueuePath,
                pullOffset,
                PartitionIndex_,
                rowBatchReadOptions,
                options)
                .Apply(BIND([this, this_ = MakeStrong(this), settings, initialOffset, pullOffset] (const NApi::TPullQueueResult& result) {
                    return ParsePullResult(result.Rowset, settings, initialOffset, pullOffset);
                })
                        .AsyncVia(GetContext()->SerializedInvoker));
        })
                .AsyncVia(GetContext()->SerializedInvoker));
}

bool TQueueSourceImpl::ConsumePullResult()
{
    auto resultOrError = std::exchange(ReadAhead_.PullFuture, {}).AsUnique().GetOrCrash();
    UnpackFailed_ = false;
    if (!resultOrError.IsOK()) {
        ReadErrorState_->SetError(TError("Failed to read from partition").With(resultOrError));
        return false;
    }
    ReadErrorState_->ClearError();

    auto& result = resultOrError.Value();
    ReadAhead_.LastPullFull = result.Full;
    ReadAhead_.PullOffset = result.NextOffset;
    for (auto& row : result.Rows) {
        ReadAhead_.DataWeight += row.DataWeight;
        ReadAhead_.Rows.push_back(std::move(row));
    }
    return true;
}

auto TQueueSourceImpl::DoReadNextBatch(
    const TMessageBatcherSettingsPtr& settings, TOffset nextOffsetAsKey, std::optional<TOffset> offsetLimitAsKey) -> TFuture<std::vector<TRecord>>
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);

    auto nextOffset = OffsetToInt(nextOffsetAsKey);
    auto offsetLimit = offsetLimitAsKey ? std::optional(OffsetToInt(*offsetLimitAsKey)) : std::nullopt;

    if (ReadAhead_.ReadOffset != nextOffset) {
        ResetReadAhead(nextOffset);
    }

    // Rows are unpacked only when taken, so unpacked data exceeds the batch limits by at most one row.
    std::vector<TRecord> records;
    i64 messageCount = 0;
    i64 byteSize = 0;
    auto isBatchFull = [&] {
        return messageCount >= settings->MaxRowsPerBatch || byteSize >= settings->MaxBytesPerBatch;
    };
    auto isOffsetLimitReached = [&] {
        i64 offset = ReadAhead_.Rows.empty() ? ReadAhead_.PullOffset : ReadAhead_.Rows.front().Offset;
        return offsetLimit && offset >= *offsetLimit;
    };
    // Returns false if a row cannot be unpacked; the row is retried by the next call.
    auto takeRows = [&] {
        while (!ReadAhead_.Rows.empty() && !isBatchFull() && !isOffsetLimitReached()) {
            const auto& packedRow = ReadAhead_.Rows.front();
            try {
                records.push_back(UnpackRecord(packedRow));
            } catch (const std::exception& ex) {
                ReadErrorState_->SetError(TError("Failed to unpack queue row")
                        .With("offset", packedRow.Offset)
                        .With(ex));
                UnpackFailed_ = true;
                return false;
            }
            // A row may unpack on retry, e.g. after a malformed message policy change.
            if (std::exchange(UnpackFailed_, false)) {
                ReadErrorState_->ClearError();
            }
            for (const auto& payload : records.back().Payloads) {
                ++messageCount;
                byteSize += sizeof(TMessage) + payload.Underlying().GetSpaceUsed();
            }
            ReadAhead_.ReadOffset = packedRow.Offset + 1;
            ReadAhead_.DataWeight -= packedRow.DataWeight;
            ReadAhead_.Rows.pop_front();
        }
        return true;
    };

    // A non-full batch keeps collecting until batch duration passes since the previous batch.
    const TCpuInstant deadline = LastBatchInstant_.value_or(GetCpuInstant()) + DurationToCpuDuration(settings->BatchDuration);
    bool finalPull = false;
    bool failed = false;
    while (!Terminated_) {
        if (ReadAhead_.PullFuture && ReadAhead_.PullFuture.IsSet() && !ConsumePullResult()) {
            failed = true;
            break;
        }
        if (!takeRows()) {
            failed = true;
            break;
        }
        if (isBatchFull() || isOffsetLimitReached() || finalPull) {
            break;
        }

        // With nothing collected, the pull is waited for up to batch duration to avoid an empty epoch.
        auto waitDuration = settings->BatchDuration;
        const TCpuInstant now = GetCpuInstant();
        if (!ReadAhead_.PullFuture && !ReadAhead_.LastPullFull) {
            // There is not much data, so do not pull repeatedly: wait out the batch duration and pull once more.
            if (now < deadline) {
                TDelayedExecutor::WaitForDuration(CpuDurationToDuration(deadline - now));
                if (Terminated_) {
                    break;
                }
            }
            finalPull = true;
        } else if (!records.empty()) {
            if (now >= deadline) {
                break;
            }
            waitDuration = CpuDurationToDuration(deadline - now);
        }

        if (!ReadAhead_.PullFuture) {
            StartPull(settings);
        }
        WaitUntilSet(
            AnySet(
                std::vector{ReadAhead_.PullFuture.AsVoid(), TDelayedExecutor::MakeDelayed(waitDuration)},
                TFutureCombinerOptions{.CancelInputOnShortcut = false}));
        if (Terminated_ || !ReadAhead_.PullFuture.IsSet()) {
            break;
        }
    }

    // Read ahead while the batch is processed.
    if (!Terminated_ &&
        !failed &&
        !ReadAhead_.PullFuture &&
        ReadAhead_.LastPullFull &&
        !isOffsetLimitReached() &&
        std::ssize(ReadAhead_.Rows) < settings->MaxRowsPerBatch &&
        ReadAhead_.DataWeight < settings->MaxBytesPerBatch)
    {
        StartPull(settings);
    }

    // Batch duration restarts after returning rows or waiting it out, not after a slow or failed pull.
    if (!records.empty() || finalPull) {
        LastBatchInstant_ = GetCpuInstant();
    }
    return MakeFuture(std::move(records));
}

auto TQueueSourceImpl::ParsePullResult(
    const NQueueClient::IQueueRowsetPtr& rowset,
    const TMessageBatcherSettingsPtr& settings,
    i64 initialOffset,
    i64 pullOffset) -> TPullResult
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);

    // The server may stop slightly below the requested limits, so a nearly full pull counts as full.
    constexpr double FullPullFraction = 0.9;

    TPullResult result{.NextOffset = pullOffset};
    if (rowset->GetRows().empty()) {
        return result;
    }
    result.Rows.reserve(rowset->GetRows().size());

    // Payload converters are cached by schema pointer, so an unchanged schema is reused.
    const auto& schema = rowset->GetSchema();
    if (!RowsetSchema_ || (RowsetSchema_ != schema && *RowsetSchema_ != *schema)) {
        RowsetSchema_ = schema;
    }

    const auto& nameTable = rowset->GetNameTable();
    const auto offsetColumnId = nameTable->GetIdOrThrow("$row_index");
    const auto timestampColumnId = nameTable->GetIdOrThrow("$timestamp");
    const auto metaColumnId = nameTable->FindId(GetParameters()->FlowQueueMetaColumn);
    i64 dataWeight = 0;
    for (auto row : rowset->GetRows()) {
        auto rowOffset = FromUnversionedValue<i64>(row[offsetColumnId]);
        i64 rowDataWeight = static_cast<i64>(NTableClient::GetDataWeight(row));
        dataWeight += rowDataWeight;
        result.NextOffset = std::max(result.NextOffset, rowOffset + 1);
        if (rowOffset < initialOffset) {
            YT_TLOG_WARNING("Got offset less than initial committed, skipped. Probably it is a start of reading old queue with empty yt flow state")
                .With("Offset", rowOffset)
                .With("InitialOffset", initialOffset);
            continue;
        }
        result.Rows.push_back({
            .Rowset = rowset,
            .Schema = RowsetSchema_,
            .Row = row,
            .TimestampColumnId = timestampColumnId,
            .MetaColumnId = metaColumnId,
            .Offset = rowOffset,
            .DataWeight = rowDataWeight,
        });
    }
    result.Full = std::ssize(rowset->GetRows()) >= settings->MaxRowsPerBatch * FullPullFraction ||
        dataWeight >= settings->MaxBytesPerBatch * FullPullFraction;

    return result;
}

auto TQueueSourceImpl::UnpackRecord(const TPackedRow& packedRow) -> TRecord
{
    const auto& row = packedRow.Row;
    auto [payloads, payloadSchema] = UnpackRow(row, packedRow.Schema);

    auto writeTime = TSystemTimestamp(
        NTransactionClient::UnixTimeFromTimestamp(FromUnversionedValue<NTransactionClient::TTimestamp>(row[packedRow.TimestampColumnId])));

    std::optional<TFlowQueueMeta> meta;
    if (packedRow.MetaColumnId && GetParameters()->TryParseFlowQueueMeta) {
        const auto raw = FromUnversionedValue<std::optional<NYson::TYsonStringBuf>>(row[*packedRow.MetaColumnId]);
        if (raw) {
            try {
                meta = ConvertTo<TFlowQueueMeta>(*raw);
            } catch (const std::exception& ex) {
                if (GetParameters()->IgnoreMalformedFlowQueueMeta) {
                    YT_TLOG_WARNING("Failed to parse flow queue meta")
                        .With("RawMeta", *raw)
                        .With(ex);
                } else {
                    THROW_ERROR_EXCEPTION("Failed to parse flow queue meta from %Qv", *raw)
                        .With(ex);
                }
            }
        }
    }

    return {
        .Offset = IntToOffset(packedRow.Offset),
        .WriteTimestamp = writeTime,
        .CreateTimestamp = writeTime,
        .Meta = meta,
        .Payloads = std::move(payloads),
        .PayloadSchema = payloadSchema,
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
