#include "servicelog.h"

#include <yt/yt/flow/library/cpp/common/resource.h>
#include <yt/yt/flow/library/cpp/common/seq_no_provider.h>

#include <yt/yt/flow/library/cpp/resources/yt_client_factory.h>

#include <yt/yt/flow/library/cpp/misc/lexicographically_serialize.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/api/rpc_proxy/helpers.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/logging/public.h>

#include <util/string/join.h>

#include <util/generic/xrange.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TServiceLogControllerState::Register(TRegistrar registrar)
{
    registrar.Parameter("ranges", &TThis::Ranges)
        .Default();
    registrar.Parameter("stray_ranges", &TThis::StrayRanges)
        .Default();
    registrar.Parameter("cached_partition_count", &TThis::CachedPartitionCount)
        .Default();
    registrar.Parameter("was_previously_finite", &TThis::WasPreviouslyFinite)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

TServiceLogSourceController::TServiceLogSourceController(
    TSourceControllerContextPtr context,
    TDynamicSourceControllerContextPtr dynamicContext)
    : TSourceControllerBase(std::move(context), std::move(dynamicContext))
    , Logger(GetContext()->Logger)
{ }

void TServiceLogSourceController::Init(IInitContextPtr initContext)
{
    initContext->InitClient<TServiceLogControllerState>(State_, "v0");
}

void TServiceLogSourceController::Sync()
{ }

void TServiceLogSourceController::Commit()
{ }

void TServiceLogSourceController::RebuildRanges(int partitionCount)
{
    State_->CachedPartitionCount = partitionCount;
    State_->Ranges.clear();
    State_->StrayRanges.clear();
    const ui64 maxHash = std::numeric_limits<ui64>::max();
    for (int i = 0; i < partitionCount; i++) {
        auto range = New<TServiceLogRange>();
        if (i != 0) {
            range->Lower = TServiceLogEndpoint();
            range->Lower->Key = MakeKey(maxHash / partitionCount * i);
        }

        if (i != partitionCount - 1) {
            range->Upper = TServiceLogEndpoint();
            range->Upper->Key = MakeKey(maxHash / partitionCount * (i + 1));
            range->Upper->Exclusive = true;
        }

        YT_LOG_INFO("RebuildRange (Number: %v, Lower: %v, Upper: %v)", i, ConvertToYsonString(range->Lower, NYson::EYsonFormat::Pretty), ConvertToYsonString(range->Upper, NYson::EYsonFormat::Pretty));
        State_->Ranges[TGuid::Create()] = range;
    }
}

void TServiceLogSourceController::EnsurePartitionsPresent()
{
    YT_LOG_INFO("Entered EnsurePartitionsPresent (RangeCount: %v)", State_->Ranges.size());
    auto dynamicSpec = GetDynamicParameters();
    if (State_->CachedPartitionCount != dynamicSpec->DesiredPartitionCount) {
        RebuildRanges(dynamicSpec->DesiredPartitionCount);
        return;
    }

    if (GetParameters()->Finite) {
        if (State_->WasPreviouslyFinite) {
            return;
        } else {
            YT_LOG_INFO("Source just transitioned into finite mode, rebuilding ranges");
            State_->WasPreviouslyFinite = true;
            RebuildRanges(dynamicSpec->DesiredPartitionCount);
        }
    } else {
        State_->WasPreviouslyFinite = false;
    }

    if (State_->Ranges.empty()) {
        YT_LOG_INFO("All ranges are terminated, rebuilding");
        RebuildRanges(dynamicSpec->DesiredPartitionCount);
    }
}

std::optional<THashMap<TKey, NYTree::IMapNodePtr>> TServiceLogSourceController::ListKeys()
{
    THashMap<TKey, NYTree::IMapNodePtr> result;
    EnsurePartitionsPresent();
    for (const auto& [id, range] : State_->Ranges) {
        auto partitionSpec = New<TDynamicServiceLogPartitionSpec>();
        partitionSpec->Range = range;
        YT_LOG_INFO("Listing keys (Key: %v, Range: %v)", id, ConvertToYsonString(range, NYson::EYsonFormat::Pretty));
        result[MakeKey(ToString(id))] = ConvertTo<NYTree::IMapNodePtr>(partitionSpec);
    }
    return result;
}

void TServiceLogSourceController::ProcessPartitionStatuses(const THashMap<TKey, TExtendedSourcePartitionStatusPtr>& statuses)
{
    for (const auto& [key, status] : statuses) {
        YT_VERIFY(status->PartitionState != EPartitionState::Interrupting);

        if (status->PartitionState == EPartitionState::Completed) {
            auto id = TGuid::FromString(FromUnversionedValue<std::string>(key.Underlying()[0]));
            if (!State_->Ranges.contains(id)) {
                // Might happen due to FlowState and ComputationController possibly not being in sync.
                continue;
            }
            auto range = std::move(State_->Ranges[id]);
            State_->StrayRanges.insert(range);
            EraseOrCrash(State_->Ranges, id);
            YT_LOG_INFO("Range terminated (Key: %v, Range: %v)", id, ConvertToYsonString(range, NYson::EYsonFormat::Pretty));
        }
    }
    YT_LOG_INFO("After processing partition statuses (RemainingRanges: %v)", State_->Ranges.size());
}

std::optional<TStreamTraverseDataPtr> TServiceLogSourceController::GetFutureKeysStreamTraverseData()
{
    auto now = NConcurrency::WaitFor(GetContext()->UniqueSeqNoProvider->Generate()).ValueOrThrow().Timestamp;

    auto sourceStream = New<TStreamTraverseData>();
    sourceStream->Epoch = -1; // Will be fixed in universal controller.

    // Servicelog is generally supposed to be "infinite" (except in tests), thus we will need a "future" draining stream.
    // That is because the logic of traversedata merge will not allow resulting streams to be in completed state if draining states are present among its parents.
    // When, nonetheless, we want a finite source, a completed stream will not block completion of other streams.
    sourceStream->State = GetParameters()->Finite ? EStreamState::Completed : EStreamState::Drained;
    sourceStream->SystemWatermark = now;
    sourceStream->EventWatermark = now;
    auto inflightMetrics = sourceStream->InflightMetrics;
    inflightMetrics->Count = 0;
    inflightMetrics->ByteSize = 0;

    return sourceStream;
}

////////////////////////////////////////////////////////////////////////////////

TServiceLogSource::TServiceLogSource(
    TSourceContextPtr context,
    TDynamicSourceContextPtr dynamicContext)
    : TOrderedSourceBase(std::move(context), std::move(dynamicContext))
    , Logger(GetContext()->Logger)
    , Range_(GetDynamicPartitionSpec()->Range)
    , TableJoiner_(
        CreateTableJoiner(
            Logger,
            GetContext()->Profiler.WithPrefix("/joiner"),
            GetContext()->StatusProfiler->WithPrefix("/joiner"),
            GetContext()->ClientsCache,
            GetParameters()->TableJoiner))
    , Throttler_(CreateReconfigurableThroughputThrottler(MakeThrottleConfig(), Logger, GetContext()->Profiler))
    , Finished_(false)
    , MinNextBatchOffset_(MakeKey())
    , NextOffset_(MakeKey())
{
    SubscribeReconfigured(
        BIND([this] (const TDynamicSourceContextPtr& /*dynamicContext*/) mutable {
            Range_ = GetDynamicPartitionSpec()->Range;
            Throttler_->Reconfigure(MakeThrottleConfig());
        }));
}

std::vector<TServiceLogSource::TRecord> TServiceLogSource::ParseData(std::vector<TPayloadRow>&& rows, NTableClient::TTableSchemaPtr schema, TSystemTimestamp now)
{
    int keySize = schema->GetKeyColumnCount();

    std::vector<TRecord> records;
    for (auto& row : rows) {
        auto key = TKey(TKey::TUnderlying(GetKeyPrefix(row, keySize)));

        size_t stringDataSize = 0;
        for (const auto& value : row) {
            if (NTableClient::IsStringLikeType(value.Type)) {
                stringDataSize += value.Length;
            }
        }

        auto payload = TPayload(TPayload::TUnderlying(
            row.GetCount(),
            stringDataSize,
            [&] (NTableClient::TMutableUnversionedRow payloadRow) {
                for (int i = 0; i < static_cast<int>(row.GetCount()); ++i) {
                    payloadRow[i] = row[i];
                    payloadRow[i].Id = i;
                }
            }));

        TRecord record{
            .Offset = CreateOffset(key, 0),
            .WriteTimestamp = now,
            .CreateTimestamp = now,
            .Meta = std::nullopt,
            .Payloads = std::vector<TPayload>{std::move(payload)},
            .PayloadSchema = schema};
        records.push_back(std::move(record));
    }
    return records;
}

TFuture<std::vector<TServiceLogSource::TRecord>> TServiceLogSource::DoReadNextBatch(
    const TMessageBatcherSettingsPtr& settings,
    TOffset nextOffset,
    std::optional<TOffset> offsetLimitExclusive)
{
    if (!Range_) {
        YT_LOG_WARNING("Range is not initialized");
        return MakeFuture<std::vector<TRecord>>(std::vector<TRecord>());
    }

    if (nextOffset == FinishedOffset_) {
        Finished_ = true;
    }

    if (Finished_) {
        YT_LOG_INFO("Reading is already finished");
        return MakeFuture<std::vector<TRecord>>(std::vector<TRecord>());
    }

    UpdatePartitionInfo(TPartitionInfoUpdate{
        .MaxOffsetExclusive = FinishedOffset_});

    nextOffset = std::max(MinNextBatchOffset_, nextOffset);
    YT_LOG_INFO("Will be reading next batch (NextOffset: %v, OffsetLimit: %v)", nextOffset, offsetLimitExclusive);
    const auto [now, uniqueSeqNo] = NConcurrency::WaitFor(GetContext()->UniqueSeqNoProvider->Generate()).ValueOrThrow();
    i64 rowLimit = settings->MaxRowsPerBatch;
    auto schema = TableJoiner_->GetSchema();
    auto effectiveRange = CloneYsonStruct(Range_);
    if (auto nextOffsetKey = TryGetOffsetKey(nextOffset)) {
        effectiveRange->Lower = TServiceLogEndpoint();
        effectiveRange->Lower->Key = *nextOffsetKey;
        effectiveRange->Lower->Exclusive = *TryGetOffsetShift(nextOffset);
    }

    if (offsetLimitExclusive && TryGetOffsetKey(*offsetLimitExclusive)) {
        effectiveRange->Upper = TServiceLogEndpoint();
        effectiveRange->Upper->Key = *TryGetOffsetKey(*offsetLimitExclusive);
        effectiveRange->Upper->Exclusive = !(*TryGetOffsetShift(*offsetLimitExclusive));
    }

    ui64 lowHash = 0;
    ui64 highHash = std::numeric_limits<ui64>::max();

    if (!ReadParsedCachedFuture_) {
        if (effectiveRange->Lower) {
            lowHash = NTableClient::FromUnversionedValue<ui64>(effectiveRange->Lower->Key.Underlying()[0]);
        }
        if (effectiveRange->Upper) {
            highHash = NTableClient::FromUnversionedValue<ui64>(effectiveRange->Upper->Key.Underlying()[0]);
        }
        ui64 diffToHighHash = highHash - lowHash;
        ui64 tryAcquireBlocks = diffToHighHash / HashBlockSize_ + 1;
        ui64 acquiredBlocks = Throttler_->TryAcquireAvailable(tryAcquireBlocks);
        ui64 effectiveHighHash = highHash;
        YT_LOG_INFO("Acquired blocks: %v", acquiredBlocks);
        if (acquiredBlocks < tryAcquireBlocks) {
            effectiveHighHash = lowHash + acquiredBlocks * HashBlockSize_;
        }

        if (!effectiveRange->Upper || MakeKey(effectiveHighHash) < effectiveRange->Upper->Key) {
            effectiveRange->Upper = TServiceLogEndpoint();
            effectiveRange->Upper->Key = MakeKey(effectiveHighHash);
            effectiveRange->Upper->Exclusive = true;
        }

        auto resultFuture = TableJoiner_->Fetch(effectiveRange, rowLimit);
        ReadParsedCachedFuture_ = resultFuture.AsUnique().Apply(BIND([this, strongThis = MakeStrong(this), schema, now, effectiveHighHash] (TFetchResult&& rawQueryResult) -> std::tuple<std::vector<TRecord>, bool, ui64> {
            auto records = ParseData(std::move(rawQueryResult.Rows), schema, now);
            return {std::move(records), rawQueryResult.Finished, effectiveHighHash};
        }));
    }

    return AnySet<void>(std::vector{ReadParsedCachedFuture_.AsVoid(),
            NConcurrency::TDelayedExecutor::MakeDelayed(settings->BatchDuration)},
        TFutureCombinerOptions{
            .PropagateCancelationToInput = false,
            .CancelInputOnShortcut = false})
        .Apply(BIND([this, strongThis = MakeStrong(this), highHash] () -> TFuture<std::vector<TRecord>> {
            if (!ReadParsedCachedFuture_.IsSet()) {
                YT_LOG_WARNING("Batch timed out. If this occurs frequently, consider increasing BatchDuration");
                return MakeFuture(std::vector<TRecord>());
            }
            auto future = std::move(ReadParsedCachedFuture_);
            ReadParsedCachedFuture_ = {};
            return future.Apply(BIND([this, strongThis = MakeStrong(this), highHash] (const std::tuple<std::vector<TRecord>, bool, ui64>& result) -> std::vector<TRecord> {
                const auto& [rows, finished, effectiveHighHash] = result;
                if (finished && effectiveHighHash == highHash) {
                    Finished_ = true;
                    YT_LOG_INFO("Reached end of range");
                }
                if (finished) {
                    MinNextBatchOffset_ = std::max(MinNextBatchOffset_, CreateOffset(MakeKey(effectiveHighHash), 0));
                }

                if (!rows.empty()) {
                    NextOffset_ = std::max(NextOffset_, GetNextOffset(rows.back().Offset));
                }
                return rows;
            }));
        }));
}

void TServiceLogSource::DoReportPersistedOffset(TOffset offsetExclusive)
{
    if (Finished_ && offsetExclusive >= NextOffset_) {
        UpdatePartitionInfo(TPartitionInfoUpdate{
            .CommittedOffsetExclusive = FinishedOffset_,
        });
        return;
    }
    UpdatePartitionInfo(TPartitionInfoUpdate{
        .CommittedOffsetExclusive = offsetExclusive,
    });
}

void TServiceLogSource::DoInit()
{ }

void TServiceLogSource::DoTerminate()
{ }

bool TServiceLogSource::IsFinite()
{
    return true;
}

i64 TServiceLogSource::DoGetEstimatedRowsAtOffset(TOffset offset)
{
    YT_LOG_INFO("Will be getting estimated rows at offset (Offset: %v)", offset);
    ui64 hash = 0;
    if (offset == FinishedOffset_) {
        hash = std::numeric_limits<ui64>::max();
        if (Range_->Upper) {
            hash = NTableClient::FromUnversionedValue<ui64>(Range_->Upper->Key.Underlying()[0]);
        }
    } else if (auto offsetKey = TryGetOffsetKey(offset)) {
        hash = NTableClient::FromUnversionedValue<ui64>(offsetKey->Underlying()[0]);
    }

    if (!ApproximateRowCountFuture_) {
        auto proc = [this, strongThis = MakeStrong(this)] {
            auto result = TableJoiner_->GetApproximateRowCount();
            YT_LOG_INFO("Retrieved ApproximateRowCount (ApproximateRowCount: %v)", result);
            return result;
        };
        ApproximateRowCountFuture_ = BIND(proc)
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

    auto delayedZero = NConcurrency::TDelayedExecutor::MakeDelayed(TDuration::Seconds(1)).Apply(BIND([] () -> i64 {
        return 0;
    }));

    auto approximateRowCount = NConcurrency::WaitFor(NYT::AnySet(std::vector{ApproximateRowCountFuture_, delayedZero}, NYT::TFutureCombinerOptions{.PropagateCancelationToInput = false, .CancelInputOnShortcut = false})).ValueOrThrow();

    double hashesPerRow = static_cast<double>(std::numeric_limits<ui64>::max()) / approximateRowCount;
    YT_LOG_INFO("ApproximateRowCount (ApproximateRowCount: %v, Hash: %v, HashesPerRow: %v)", approximateRowCount, hash, hashesPerRow);

    return hash / hashesPerRow;
}

bool TServiceLogSource::AreOffsetsConsecutive() const
{
    return false;
}

bool TServiceLogSource::CanCommittedOffsetExceedNextReadOffset() const
{
    return true;
}

TOffset TServiceLogSource::GetNextOffset(const TOffset& offset) const
{
    YT_VERIFY(offset != FinishedOffset_);
    TKey keyPart = TryGetOffsetKey(offset).value_or(MakeKey());
    i64 shiftPart = TryGetOffsetShift(offset).value_or(0);

    return CreateOffset(keyPart, shiftPart + 1);
}

std::string TServiceLogSource::ConvertOffsetToLexicographicallyComparableString(const TOffset& offset) const
{
    return LexicographicallySerializeUnversionedRowV1(offset.Underlying().Get());
}

std::optional<TKey> TServiceLogSource::TryGetOffsetKey(const TOffset& offset) const
{
    const auto& row = offset.Underlying();
    if (row.GetCount() != 0) {
        auto builder = NTableClient::TUnversionedOwningRowBuilder();
        for (int index = 1; index < row.GetCount() - 1; ++index) {
            builder.AddValue(row[index]);
        }
        return TKey(TKey::TUnderlying(builder.FinishRow()));
    }
    return std::nullopt;
}

std::optional<i64> TServiceLogSource::TryGetOffsetShift(const TOffset& offset) const
{
    const auto& row = offset.Underlying();
    if (row.GetCount() != 0) {
        return NTableClient::FromUnversionedValue<i64>(row[row.GetCount() - 1]);
    }
    return std::nullopt;
}

TOffset TServiceLogSource::CreateOffset(const TKey& key, i64 shift) const
{
    if (key.Underlying().GetCount() == 0 && shift == 0) {
        // We want default-constructed offset be the same as offset = 0.
        return MakeKey();
    }

    NTableClient::TUnversionedRowBuilder builder;
    builder.AddValue(NTableClient::MakeUnversionedInt64Value(0));
    for (auto& value : key.Underlying()) {
        builder.AddValue(value);
    }
    builder.AddValue(NTableClient::MakeUnversionedInt64Value(shift));
    return TOffset(TOffset::TUnderlying(builder.GetRow()));
}

const TOffset TServiceLogSource::FinishedOffset_ = MakeKey(1);

NConcurrency::TThroughputThrottlerConfigPtr TServiceLogSource::MakeThrottleConfig() const
{
    if (!Range_) {
        THROW_ERROR_EXCEPTION("Range is not initialized");
    }
    ui64 lowerHash = 0;
    ui64 upperHash = std::numeric_limits<ui64>::max();
    if (Range_->Lower) {
        lowerHash = FromUnversionedValue<ui64>(Range_->Lower->Key.Underlying()[0]);
    }
    if (Range_->Upper) {
        YT_VERIFY(Range_->Upper->Exclusive);
        upperHash = FromUnversionedValue<ui64>(Range_->Upper->Key.Underlying()[0]);
    }

    ui64 blockCount = (upperHash - lowerHash) / HashBlockSize_;
    double blocksPerSecond = blockCount / GetDynamicParameters()->DesiredCycleTime.SecondsFloat();
    YT_LOG_INFO("Throttler limit reset (BlocksPerSecond: %v)", blocksPerSecond);

    auto throttlerConfig = New<NConcurrency::TThroughputThrottlerConfig>();
    throttlerConfig->Limit = blocksPerSecond;
    throttlerConfig->Period = GetDynamicParameters()->ThrottlerPeriod;
    return throttlerConfig;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
