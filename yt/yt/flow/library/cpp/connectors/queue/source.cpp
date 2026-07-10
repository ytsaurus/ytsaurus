#include "source.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/schema.h>

#include <yt/yt/flow/library/cpp/connectors/common/flow_queue_meta.h>

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

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

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
            << TErrorAttribute("queue_key", key);
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
        GetContext()->Logger.WithTag("QueuePath: %v", GetParameters()->QueuePath),
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
        YT_LOG_DEBUG("Skipped some partitions due to filter (Skipped: %v, Left: %v)", skipped, std::size(keys));
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
    , Logger(TOrderedSourceBase::Logger.WithTag("QueuePath: %v, ConsumerPath: %v, PartitionIndex: %v",
        GetParameters()->QueuePath,
        GetParameters()->ConsumerPath,
        PartitionIndex_))
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
    if (CurrentRequestFuture_) {
        CurrentRequestFuture_.Cancel(TError("Source is terminated, so result of current request is useless"));
        CurrentRequestFuture_ = {};
    }
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
    auto replicasNode = NYTree::ConvertToNode(replicasYson)->AsMap();

    for (const auto& [replicaId, descriptor] : replicasNode->GetChildren()) {
        const auto& attributes = descriptor->AsMap();
        if (attributes->GetChildOrThrow("mode")->AsString()->GetValue() != "sync") {
            continue;
        }
        if (auto stateChild = attributes->FindChild("state");
            stateChild && stateChild->AsString()->GetValue() != "enabled")
        {
            continue;
        }
        return TSyncReplica{
            .Client = GetContext()->ClientsCache->GetClient(
                attributes->GetChildOrThrow("cluster_name")->AsString()->GetValue()),
            .Path = NYPath::TYPath(attributes->GetChildOrThrow("replica_path")->AsString()->GetValue()),
        };
    }

    THROW_ERROR_EXCEPTION("No enabled synchronous queue replica found for replicated queue %v", queuePath);
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
        auto error = TError("Failed to update partition info") << TError(ex);
        YT_LOG_ERROR(error);
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

auto TQueueSourceImpl::DoReadNextBatch(
    const TMessageBatcherSettingsPtr& settings, TOffset nextOffsetAsKey, std::optional<TOffset> offsetLimitAsKey) -> TFuture<std::vector<TRecord>>
{
    auto nextOffset = OffsetToInt(nextOffsetAsKey);
    auto offsetLimit = offsetLimitAsKey ? std::optional(OffsetToInt(*offsetLimitAsKey)) : std::nullopt;

    const auto actualParameters = std::pair(settings, nextOffset);

    if (CurrentRequestFuture_ && actualParameters != CurrentRequestParameters_) {
        CurrentRequestFuture_.Cancel(TError("Reading batch parameters have changed, so result of current request is useless"));
        CurrentRequestFuture_ = {};
    }

    if (!CurrentRequestFuture_) {
        CurrentRequestParameters_ = actualParameters;

        NQueueClient::TQueueRowBatchReadOptions rowBatchReadOptions;
        rowBatchReadOptions.MaxRowCount = settings->MaxRowsPerBatch;
        rowBatchReadOptions.MaxDataWeight = settings->MaxBytesPerBatch;

        CurrentRequestFuture_ = InitialCommittedOffset_
            .ToFuture()
            .ToUncancelable()
            .Apply(BIND([this, this_ = MakeStrong(this), offsetLimit, nextOffset, rowBatchReadOptions] (const i64& initialOffset) {
                NApi::TPullQueueConsumerOptions options;
                options.Timeout = GetDynamicParameters()->PullQueueTimeout;
                // A no-op for a plain ordered table; for a replicated/chaos queue it forces reading a synchronous replica.
                options.ReplicaConsistency = NApi::EReplicaConsistency::Sync;

                // Optimistically start reading from nextOffset (drop messages with offset less than initial later).
                // TODO(mikari): consumer or queue client?
                return ConsumerClient_->PullQueueConsumer(
                    GetParameters()->ConsumerPath,
                    GetParameters()->QueuePath,
                    nextOffset,
                    PartitionIndex_,
                    rowBatchReadOptions,
                    options)
                    .Apply(BIND([this, this_ = MakeStrong(this), offsetLimit, initialOffset] (const NQueueClient::IQueueRowsetPtr& rowset) {
                        return ParseData(rowset, initialOffset, offsetLimit);
                    })
                            .AsyncVia(GetContext()->SerializedInvoker));
            })
                    .AsyncVia(GetContext()->SerializedInvoker));
    }

    return AnySet(
        std::vector{CurrentRequestFuture_.AsVoid(), NConcurrency::TDelayedExecutor::MakeDelayed(settings->BatchDuration)},
        TFutureCombinerOptions{.CancelInputOnShortcut = false})
        .Apply(
            BIND([this, this_ = MakeStrong(this)] (const TError&) {
                if (CurrentRequestFuture_.IsSet()) {
                    auto future = std::move(CurrentRequestFuture_);
                    CurrentRequestFuture_ = {};
                    if (future.GetOrCrash().IsOK()) {
                        GetReadErrorState()->ClearError();
                        return future;
                    } else {
                        auto error = TError("Failed to read from partition") << future.GetOrCrash();
                        YT_LOG_ERROR(error);
                        GetReadErrorState()->SetError(error);
                    }
                }
                return MakeFuture(std::vector<TQueueSourceImpl::TRecord>{});
            })
                .AsyncVia(GetContext()->SerializedInvoker));
}

auto TQueueSourceImpl::ParseData(
    const NQueueClient::IQueueRowsetPtr& rowset, i64 initialOffset, std::optional<i64> offsetLimit) -> std::vector<TRecord>
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);

    std::vector<TRecord> records;
    records.reserve(rowset->GetRows().size());

    if (rowset->GetRows().size() == 0) {
        return {};
    }

    const auto& nameTable = rowset->GetNameTable();
    const auto offsetColumnId = nameTable->GetIdOrThrow("$row_index");
    const auto timestampColumnId = nameTable->GetIdOrThrow("$timestamp");
    const auto metaColumnId = nameTable->FindId(GetParameters()->FlowQueueMetaColumn);

    for (auto row : rowset->GetRows()) {
        auto rowOffset = FromUnversionedValue<i64>(row[offsetColumnId]);
        if (rowOffset < initialOffset) {
            YT_LOG_WARNING("Got offset less than initial committed, skipped. "
                "Probably it is a start of reading old queue with empty yt flow state "
                "(Offset: %v, InitialOffset: %v)",
                rowOffset,
                initialOffset);
            continue;
        }
        if (offsetLimit && rowOffset >= *offsetLimit) {
            YT_LOG_WARNING("Got offset bigger than limit, skipped (Offset: %v, OffsetLimit: %v)",
                rowOffset,
                *offsetLimit);
            continue;
        }

        auto [payloads, payloadSchema] = UnpackRow(row, rowset->GetSchema());

        auto writeTime = TSystemTimestamp(
            NTransactionClient::UnixTimeFromTimestamp(FromUnversionedValue<NTransactionClient::TTimestamp>(row[timestampColumnId])));

        std::optional<TFlowQueueMeta> meta;
        if (metaColumnId && GetParameters()->TryParseFlowQueueMeta) {
            const auto raw = FromUnversionedValue<std::optional<NYson::TYsonStringBuf>>(row[*metaColumnId]);
            if (raw) {
                try {
                    meta = ConvertTo<TFlowQueueMeta>(*raw);
                } catch (const std::exception& ex) {
                    if (GetParameters()->IgnoreMalformedFlowQueueMeta) {
                        YT_LOG_WARNING(ex, "Failed to parse flow queue meta from %Qv", *raw);
                    } else {
                        THROW_ERROR_EXCEPTION("Failed to parse flow queue meta from %Qv", *raw)
                            << TError(ex);
                    }
                }
            }
        }
        TRecord record = {
            .Offset = IntToOffset(rowOffset),
            .WriteTimestamp = writeTime,
            .CreateTimestamp = writeTime,
            .Meta = meta,
            .Payloads = payloads,
            .PayloadSchema = payloadSchema};
        records.push_back(std::move(record));
    }

    return records;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
