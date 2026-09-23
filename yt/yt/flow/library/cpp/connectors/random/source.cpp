#include "source.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>

#include <yt/yt/client/table_client/schema.h>

#include <util/generic/xrange.h>
#include <util/random/fast.h>

#include <cstring>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

TFingerprint ComputeSeedFingerprint(const TSourceContextPtr& context)
{
    YT_VERIFY(context->SourceKey);
    return FarmFingerprint(
        FarmFingerprint(TStringBuf(context->PipelinePath.GetPath())),
        GetFarmFingerprint(context->SourceKey.Underlying().Get()));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TRandomSource::TRandomSource(
    TSourceContextPtr context,
    TDynamicSourceContextPtr dynamicContext)
    : TIntegerOffsetOrderedSourceBase(std::move(context), std::move(dynamicContext))
    , Schema_(
        New<NTableClient::TTableSchema>(
            std::vector{
                NTableClient::TColumnSchema("key", EValueType::String),
                NTableClient::TColumnSchema("data", EValueType::String),
            }))
    , KeyId_(Schema_->GetColumnIndexOrThrow("key"))
    , DataId_(Schema_->GetColumnIndexOrThrow("data"))
    , SeedFingerprint_(ComputeSeedFingerprint(GetContext()))
    , Generator_(0)
{
    UpdatePartitionInfo(TPartitionInfoUpdate{.CommittedOffsetExclusive = IntToOffset(0)});
}

void TRandomSource::DoInit()
{
    auto now = TInstant::Now();
    GeneratedCount_.Update(0, now);
    GeneratedBytes_.Update(0, now);
}

void TRandomSource::DoReportPersistedOffset(TOffset offsetExclusive)
{
    auto partitionMessageCount = GetDynamicParameters()->PartitionMessageCount;
    UpdatePartitionInfo(
        TPartitionInfoUpdate{
            .CommittedOffsetExclusive = offsetExclusive,
            .MaxOffsetExclusive = partitionMessageCount ? std::optional(IntToOffset(*partitionMessageCount)) : std::nullopt,
        });
}

std::optional<TBacklogRate> TRandomSource::EstimateBacklogRate()
{
    const auto& parameters = *GetDynamicParameters();
    if (parameters.ReportedBacklogBytesPerSecond) {
        return TBacklogRate{
            .BytesPerSecond = *parameters.ReportedBacklogBytesPerSecond,
            .MessagesPerSecond = parameters.ReportedBacklogMessagesPerSecond.value_or(0.0),
        };
    }

    // Random data is produced on reads, so reading throughput estimates its arrival rate.
    auto now = TInstant::Now();
    auto countRate = GeneratedCount_.GetDecayedRate(now);
    auto byteRate = GeneratedBytes_.GetDecayedRate(now);
    if (!countRate || !byteRate) {
        return std::nullopt;
    }
    return TBacklogRate{
        .BytesPerSecond = *byteRate,
        .MessagesPerSecond = *countRate,
    };
}

TFuture<std::vector<TRandomSource::TRecord>> TRandomSource::DoReadNextBatch(const TMessageBatcherSettingsPtr& settings, TOffset nextOffsetAsKey, std::optional<TOffset> offsetLimitAsKey)
{
    const auto& params = *GetDynamicParameters();

    i64 nextOffset = OffsetToInt(nextOffsetAsKey);
    std::optional<i64> offsetLimit = offsetLimitAsKey ? std::optional(OffsetToInt(*offsetLimitAsKey)) : std::nullopt;
    if (params.PartitionMessageCount.has_value()) {
        if (offsetLimit.has_value()) {
            offsetLimit = std::min<i64>(*offsetLimit, *params.PartitionMessageCount);
        } else {
            offsetLimit = *params.PartitionMessageCount;
        }
    }

    std::poisson_distribution<i64> messageCountDistribution(params.MessageCountMean);

    i64 bytes = 0;
    i64 count = std::min<i64>(settings->MaxRowsPerBatch, messageCountDistribution(Generator_));
    std::vector<TRecord> records;
    records.reserve(count);

    auto now = TSystemTimestamp(TInstant::Now().Seconds());

    for (i64 i = 0; i < count && bytes < settings->MaxBytesPerBatch && (!offsetLimit || nextOffset < *offsetLimit); ++i) {
        // The record content is a pure function of the pipeline path, the partition and the offset: message ids
        // derive from the offset, and a Swift source computation re-reads the offset after a restart expecting
        // the same record. The pipeline path keeps the streams of different pipelines apart.
        TFastRng64 recordGenerator(FarmFingerprint(SeedFingerprint_, nextOffset));
        std::poisson_distribution<i64> messageSizeDistribution(params.MessageSizeMean);
        std::poisson_distribution<i64> keyDistribution(params.MessageKeyRange);

        // Clamp message size, it must be strictly limited.
        const i64 strLen = std::min<i64>(messageSizeDistribution(recordGenerator), params.MessageSizeMean * 10);
        auto randomString = std::string(strLen + 3, 0);
        char* p = randomString.data();
        for (i64 j = 0; j < strLen; j += 4) {
            ui32 r = recordGenerator() | 0x01010101u;
            std::memcpy(p + j, &r, 4);
        }
        randomString.resize(strLen);

        auto randomKey = ToString(keyDistribution(recordGenerator));

        auto payload = TPayload(TPayload::TUnderlying(
            Schema_->GetColumnCount(),
            randomKey.size() + randomString.size(),
            [&] (TMutableUnversionedRow row) {
                row[KeyId_] = MakeUnversionedStringValue(randomKey, KeyId_);
                row[DataId_] = MakeUnversionedStringValue(randomString, DataId_);
            }));

        TRecord record = {
            .Offset = IntToOffset(nextOffset),
            .WriteTimestamp = now,
            .CreateTimestamp = now,
            .Meta = std::nullopt,
            .Payloads = {std::move(payload)},
            .PayloadSchema = Schema_};

        bytes += record.Payloads[0].Underlying().GetSpaceUsed();
        records.push_back(std::move(record));
        nextOffset += 1;
    }

    auto generatedAt = TInstant::Now();
    GeneratedCount_.Inc(std::ssize(records), generatedAt);
    GeneratedBytes_.Inc(bytes, generatedAt);

    return MakeFuture(std::move(records));
}

////////////////////////////////////////////////////////////////////////////////

std::optional<THashMap<TKey, NYTree::IMapNodePtr>> TRandomSourceController::ListKeys()
{
    auto trivialSpec = GetEphemeralNodeFactory()->CreateMap();
    THashMap<TKey, NYTree::IMapNodePtr> keys;
    for (int i = 0; i < GetDynamicParameters()->PartitionCount; ++i) {
        keys[MakeKey(i)] = trivialSpec;
    }
    return keys;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
