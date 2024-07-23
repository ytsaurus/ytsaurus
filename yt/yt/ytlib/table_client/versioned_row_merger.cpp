#include "versioned_row_merger.h"

#include "config.h"
#include "private.h"

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/unversioned_value.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/client/tablet_client/watermark_runtime_data.h>

#include <yt/yt/client/transaction_client/helpers.h>

namespace NYT::NTableClient {

using namespace NQueryClient;
using namespace NTransactionClient;
using namespace NTabletClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TVersionedRowMerger
    : public IVersionedRowMerger
{
public:
    TVersionedRowMerger(
        TRowBufferPtr rowBuffer,
        int columnCount,
        int keyColumnCount,
        const TColumnFilter& columnFilter,
        TRetentionConfigPtr config,
        TTimestamp currentTimestamp,
        TTimestamp majorTimestamp,
        TColumnEvaluatorPtr columnEvaluator,
        bool mergeRowsOnFlush,
        std::optional<int> ttlColumnIndex,
        bool mergeDeletionsOnFlush,
        std::optional<TWatermarkRuntimeData> watermarkRuntimeData)
        : RowBuffer_(std::move(rowBuffer))
        , KeyColumnCount_(keyColumnCount)
        , Config_(std::move(config))
        , IgnoreMajorTimestamp_(Config_ ? Config_->IgnoreMajorTimestamp : false)
        , CurrentTimestamp_(currentTimestamp)
        , MajorTimestamp_(IgnoreMajorTimestamp_ ? MaxTimestamp : majorTimestamp)
        , ColumnEvaluator_(std::move(columnEvaluator))
        , MergeRowsOnFlush_(mergeRowsOnFlush)
        , MergeDeletionsOnFlush_(mergeDeletionsOnFlush)
        , TtlColumnIndex_(ttlColumnIndex)
        , WatermarkRuntimeData_(std::move(watermarkRuntimeData))
    {
        int mergedKeyColumnCount = 0;
        if (columnFilter.IsUniversal()) {
            for (int id = 0; id < columnCount; ++id) {
                if (id < keyColumnCount) {
                    ++mergedKeyColumnCount;
                }
                ColumnIds_.push_back(id);
            }
            YT_ASSERT(mergedKeyColumnCount == keyColumnCount);
        } else {
            for (int id : columnFilter.GetIndexes()) {
                if (id < keyColumnCount) {
                    ++mergedKeyColumnCount;
                }
                ColumnIds_.push_back(id);
            }
        }

        ColumnIdToIndex_.resize(columnCount, std::numeric_limits<int>::max());

        for (int index = 0; index < static_cast<int>(ColumnIds_.size()); ++index) {
            int id = ColumnIds_[index];
            if (id >= KeyColumnCount_) {
                ColumnIdToIndex_[id] = index;
            }
        }

        Keys_.resize(mergedKeyColumnCount);

        Cleanup();
    }

    void AddPartialRow(TVersionedRow row, TTimestamp upperTimestampLimit) override
    {
        if (!row) {
            return;
        }

        if (!Started_) {
            Started_ = true;
            YT_ASSERT(row.GetKeyCount() == KeyColumnCount_);
            for (int index = 0; index < std::ssize(ColumnIds_); ++index) {
                int id = ColumnIds_[index];
                if (id < KeyColumnCount_) {
                    YT_ASSERT(index < std::ssize(Keys_));
                    Keys_.data()[index] = row.Keys()[id];
                }
            }
        }

        for (const auto& value : row.Values()) {
            if (value.Timestamp < upperTimestampLimit) {
                PartialValues_.push_back(value);
            }
        }

        for (auto timestamp : row.DeleteTimestamps()) {
            if (timestamp < upperTimestampLimit) {
                DeleteTimestamps_.push_back(timestamp);
            }
        }
    }

    TMutableVersionedRow BuildMergedRow(bool produceEmptyRow) override
    {
        if (!Started_) {
            return {};
        }

        // Sort delete timestamps in ascending order and remove duplicates.
        std::sort(DeleteTimestamps_.begin(), DeleteTimestamps_.end());
        DeleteTimestamps_.erase(
            std::unique(DeleteTimestamps_.begin(), DeleteTimestamps_.end()),
            DeleteTimestamps_.end());

        // Sort input values by |(id, timestamp)| and remove duplicates.
        std::sort(
            PartialValues_.begin(),
            PartialValues_.end(),
            [&] (const TVersionedValue& lhs, const TVersionedValue& rhs) {
                auto lhsIndex = ColumnIdToIndex_[lhs.Id];
                auto rhsIndex = ColumnIdToIndex_[rhs.Id];
                return std::tie(lhsIndex, lhs.Id, lhs.Timestamp) < std::tie(rhsIndex, rhs.Id, rhs.Timestamp);
            });
        PartialValues_.erase(
            std::unique(
                PartialValues_.begin(),
                PartialValues_.end(),
                [] (const TVersionedValue& lhs, const TVersionedValue& rhs) {
                    return std::tie(lhs.Id, lhs.Timestamp) == std::tie(rhs.Id, rhs.Timestamp);
                }),
            PartialValues_.end());

        auto rowMaxDataTtl = ComputeRowMaxDataTtl();
        auto watermarkTimestamp = ComputeWatermarkTimestamp();

        // Scan through input values.
        auto columnIdsBeginIt = ColumnIds_.begin();
        auto columnIdsEndIt = ColumnIds_.end();
        auto partialValueIt = PartialValues_.begin();
        while (partialValueIt != PartialValues_.end()) {
            // Extract a range of values for the current column.
            auto columnBeginIt = partialValueIt;
            auto columnEndIt = partialValueIt + 1;
            while (columnEndIt != PartialValues_.end() && columnEndIt->Id == partialValueIt->Id) {
                ++columnEndIt;
            }

            // Skip values if the current column is filtered out.
            while (columnIdsBeginIt != columnIdsEndIt && *columnIdsBeginIt != partialValueIt->Id) {
                ++columnIdsBeginIt;
            }

            bool needToSaveColumn = columnIdsBeginIt != columnIdsEndIt;

            // Merge with delete timestamps and put result into ColumnValues_.
            // Delete timestamps are represented by TheBottom sentinels.
            {
                ColumnValues_.clear();
                auto timestampBeginIt = DeleteTimestamps_.begin();
                auto timestampEndIt = DeleteTimestamps_.end();
                auto columnValueIt = columnBeginIt;
                auto timestampIt = timestampBeginIt;
                while (columnValueIt != columnEndIt || timestampIt != timestampEndIt) {
                    if (timestampIt == timestampEndIt ||
                        (columnValueIt != columnEndIt && columnValueIt->Timestamp < *timestampIt))
                    {
                        ColumnValues_.push_back(*columnValueIt++);
                    } else {
                        auto value = MakeVersionedSentinelValue(EValueType::TheBottom, *timestampIt);
                        ColumnValues_.push_back(value);
                        ++timestampIt;
                    }
                }
            }

#ifndef NDEBUG
            // Validate merged list.
            for (auto it = ColumnValues_.begin(); it != ColumnValues_.end() - 1; ++it) {
                YT_ASSERT(it->Timestamp <= (it + 1)->Timestamp);
            }
#endif

            auto retentionBeginIt = Config_ || watermarkTimestamp ? ColumnValues_.end() : ColumnValues_.begin();

            // Apply retention config if present.
            if (Config_) {
                YT_VERIFY(rowMaxDataTtl);

                // Compute safety limit by MinDataTtl.
                while (retentionBeginIt != ColumnValues_.begin()) {
                    auto timestamp = (retentionBeginIt - 1)->Timestamp;
                    if (timestamp < CurrentTimestamp_ &&
                        TimestampDiffToDuration(timestamp, CurrentTimestamp_).first >= Config_->MinDataTtl)
                    {
                        break;
                    }
                    --retentionBeginIt;
                }

                // Adjust safety limit by MinDataVersions.
                if (std::distance(ColumnValues_.begin(), retentionBeginIt) > Config_->MinDataVersions) {
                    retentionBeginIt -= Config_->MinDataVersions;
                } else {
                    retentionBeginIt = ColumnValues_.begin();
                }

                // Compute retention limit by MaxDataVersions and MaxDataTtl.
                while (retentionBeginIt != ColumnValues_.begin()) {
                    if (std::distance(retentionBeginIt, ColumnValues_.end()) >= Config_->MaxDataVersions) {
                        break;
                    }

                    auto timestamp = (retentionBeginIt - 1)->Timestamp;
                    if (timestamp < CurrentTimestamp_ &&
                        TimestampDiffToDuration(timestamp, CurrentTimestamp_).first > rowMaxDataTtl)
                    {
                        break;
                    }

                    --retentionBeginIt;
                }
            }

            // For aggregate columns merge values before MajorTimestamp_ and leave other values.
            int id = partialValueIt->Id;
            if (ColumnEvaluator_->IsAggregate(id) && retentionBeginIt < ColumnValues_.end()) {

                // TODO(lukyan): Use MajorTimestamp_ == int max for MergeRowsOnFlush_.
                while (retentionBeginIt != ColumnValues_.begin() && retentionBeginIt->Timestamp >= MajorTimestamp_ && !MergeRowsOnFlush_)
                {
                    --retentionBeginIt;
                }

                if (retentionBeginIt > ColumnValues_.begin()) {
                    auto valueIt = retentionBeginIt;
                    while (true) {
                        if (valueIt->Type == EValueType::TheBottom) {
                            ++valueIt;
                            break;
                        }

                        if (None(valueIt->Flags & EValueFlags::Aggregate)) {
                            break;
                        }

                        if (valueIt == ColumnValues_.begin()) {
                            break;
                        }

                        --valueIt;
                    }

                    if (valueIt < retentionBeginIt) {
                        // NB: RHS is versioned.
                        TUnversionedValue state = *valueIt++;

                        // The very first aggregated value determines the final aggregation mode.
                        // Preserve initial aggregate flag.
                        auto initialAggregateFlags = state.Flags & EValueFlags::Aggregate;

                        for (; valueIt <= retentionBeginIt; ++valueIt) {
                            const auto& value = *valueIt;

                            // Do no expect any tombstones.
                            YT_ASSERT(value.Type != EValueType::TheBottom);
                            // Only expect overwrites at the very beginning.
                            YT_ASSERT(Any(value.Flags & EValueFlags::Aggregate));

                            ColumnEvaluator_->MergeAggregate(id, &state, value, RowBuffer_);

                            // Preserve aggregate flag in aggregate functions.
                            state.Flags &= ~EValueFlags::Aggregate;
                            state.Flags |= initialAggregateFlags;
                        }

                        // Value is not finalized yet. Further merges may happen.
                        YT_ASSERT((state.Flags & EValueFlags::Aggregate) == initialAggregateFlags);
                        static_cast<TUnversionedValue&>(*retentionBeginIt) = state;
                    }
                }
            }

            if (watermarkTimestamp) {
                while (retentionBeginIt != ColumnValues_.begin()) {
                    if ((retentionBeginIt - 1)->Timestamp <= *watermarkTimestamp) {
                        break;
                    }

                    --retentionBeginIt;
                }
            }

            // Save output values and timestamps.
            for (auto it = ColumnValues_.rbegin(); it.base() != retentionBeginIt; ++it) {
                if (it->Type != EValueType::TheBottom) {
                    WriteTimestamps_.push_back(it->Timestamp);
                    if (needToSaveColumn) {
                        MergedValues_.push_back(*it);
                    }
                }
            }

            partialValueIt = columnEndIt;
        }

        // Sort write timestamps in ascending order, remove duplicates.
        std::sort(WriteTimestamps_.begin(), WriteTimestamps_.end());
        WriteTimestamps_.erase(
            std::unique(WriteTimestamps_.begin(), WriteTimestamps_.end()),
            WriteTimestamps_.end());

        // Delete redundant delete timestamps between subsequent write timestamps.
        if (MergeRowsOnFlush_ && MergeDeletionsOnFlush_) {
            auto nextWriteTimestampIt = WriteTimestamps_.begin();
            auto deleteTimestampOutputIt = DeleteTimestamps_.begin();
            bool deleteTimestampStored = false;

            for (auto deleteTimestamp : DeleteTimestamps_) {
                while (nextWriteTimestampIt != WriteTimestamps_.end() && *nextWriteTimestampIt <= deleteTimestamp) {
                    nextWriteTimestampIt++;
                    deleteTimestampStored = false;
                }

                if (!deleteTimestampStored) {
                    *deleteTimestampOutputIt++ = deleteTimestamp;
                    deleteTimestampStored = true;
                }
            }

            DeleteTimestamps_.erase(deleteTimestampOutputIt, DeleteTimestamps_.end());
        }

        // Reverse write and delete timestamps list to make them appear in descending order.
        std::reverse(DeleteTimestamps_.begin(), DeleteTimestamps_.end());
        std::reverse(WriteTimestamps_.begin(), WriteTimestamps_.end());

        // Delete redundant tombstones preceding major timestamp.
        {
            auto earliestWriteTimestamp = WriteTimestamps_.empty()
                ? MaxTimestamp
                : WriteTimestamps_.back();
            auto it = DeleteTimestamps_.begin();
            while (it != DeleteTimestamps_.end() && (*it > earliestWriteTimestamp || *it >= MajorTimestamp_)) {
                ++it;
            }
            DeleteTimestamps_.erase(it, DeleteTimestamps_.end());
        }

        if (!produceEmptyRow && MergedValues_.empty() && WriteTimestamps_.empty() && DeleteTimestamps_.empty()) {
            Cleanup();
            return {};
        }

        // Construct output row.
        auto row = RowBuffer_->AllocateVersioned(
            Keys_.size(),
            MergedValues_.size(),
            WriteTimestamps_.size(),
            DeleteTimestamps_.size());

        // Construct output keys.
        std::copy(Keys_.begin(), Keys_.end(), row.BeginKeys());

        // Construct output values.
        std::copy(MergedValues_.begin(), MergedValues_.end(), row.BeginValues());

        // Construct output timestamps.
        std::copy(WriteTimestamps_.begin(), WriteTimestamps_.end(), row.BeginWriteTimestamps());
        std::copy(DeleteTimestamps_.begin(), DeleteTimestamps_.end(), row.BeginDeleteTimestamps());

        Cleanup();

        return row;
    }

    void Reset() override
    {
        YT_ASSERT(!Started_);
        RowBuffer_->Clear();
    }


private:
    const TRowBufferPtr RowBuffer_;
    const int KeyColumnCount_;
    const TRetentionConfigPtr Config_;
    const bool IgnoreMajorTimestamp_;
    const TTimestamp CurrentTimestamp_;
    const TTimestamp MajorTimestamp_;
    const TColumnEvaluatorPtr ColumnEvaluator_;
    const bool MergeRowsOnFlush_;
    const bool MergeDeletionsOnFlush_;
    const std::optional<int> TtlColumnIndex_;
    const std::optional<TWatermarkRuntimeData> WatermarkRuntimeData_;

    bool Started_ = false;

    TCompactVector<int, TypicalColumnCount> ColumnIds_;
    TCompactVector<int, TypicalColumnCount> ColumnIdToIndex_;
    TCompactVector<TUnversionedValue, TypicalColumnCount> Keys_;

    std::vector<TVersionedValue> PartialValues_;
    std::vector<TVersionedValue> ColumnValues_;
    std::vector<TVersionedValue> MergedValues_;

    std::vector<TTimestamp> WriteTimestamps_;
    std::vector<TTimestamp> DeleteTimestamps_;

    void Cleanup()
    {
        PartialValues_.clear();
        MergedValues_.clear();
        ColumnValues_.clear();

        WriteTimestamps_.clear();
        DeleteTimestamps_.clear();

        Started_ = false;
    }

    std::optional<TDuration> ComputeRowMaxDataTtl() const
    {
        if (!Config_) {
            return std::nullopt;
        }

        auto ttlColumnIt = std::find_if(
            PartialValues_.rbegin(),
            PartialValues_.rend(),
            [&] (const TVersionedValue& value) {
                return value.Id == TtlColumnIndex_;
            });

        if (ttlColumnIt != PartialValues_.rend() && ttlColumnIt->Type == EValueType::Uint64 &&
            (DeleteTimestamps_.empty() || ttlColumnIt->Timestamp > DeleteTimestamps_.back()))
        {
            return std::max(Config_->MinDataTtl, TDuration::MilliSeconds(FromUnversionedValue<ui64>(*ttlColumnIt)));
        }

        return Config_->MaxDataTtl;
    }

    std::optional<TTimestamp> ComputeWatermarkTimestamp() const
    {
        if (!WatermarkRuntimeData_) {
            return {};
        }

        TTimestamp watermarkTimestamp = MinTimestamp;
        for (const auto& value : PartialValues_) {
            if (value.Id != WatermarkRuntimeData_->ColumnIndex) {
                continue;
            }

            if (value.Type != EValueType::Null && FromUnversionedValue<ui64>(value) < WatermarkRuntimeData_->Watermark) {
                watermarkTimestamp = std::max(watermarkTimestamp, value.Timestamp);
            }
        }

        return watermarkTimestamp;
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedRowMerger> CreateLegacyVersionedRowMerger(
    TRowBufferPtr rowBuffer,
    int columnCount,
    int keyColumnCount,
    const TColumnFilter& columnFilter,
    TRetentionConfigPtr config,
    TTimestamp currentTimestamp,
    TTimestamp majorTimestamp,
    NQueryClient::TColumnEvaluatorPtr columnEvaluator,
    bool mergeRowsOnFlush,
    std::optional<int> ttlColumnIndex,
    bool mergeDeletionsOnFlush)
{
    return std::make_unique<TVersionedRowMerger>(
        rowBuffer,
        columnCount,
        keyColumnCount,
        columnFilter,
        config,
        currentTimestamp,
        majorTimestamp,
        columnEvaluator,
        mergeRowsOnFlush,
        ttlColumnIndex,
        mergeDeletionsOnFlush,
        /*runtimeData*/ std::nullopt);
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedRowMerger> CreateVersionedRowMerger(
    ERowMergerType rowMergerType,
    TRowBufferPtr rowBuffer,
    TTableSchemaPtr tableSchema,
    const TColumnFilter& columnFilter,
    TRetentionConfigPtr config,
    TTimestamp currentTimestamp,
    TTimestamp majorTimestamp,
    TColumnEvaluatorPtr columnEvaluator,
    NYson::TYsonString customRuntimeData,
    bool mergeRowsOnFlush,
    bool useTtlColumn,
    bool mergeDeletionsOnFlush)
{
    switch (rowMergerType) {
        case ERowMergerType::Legacy:
            return CreateLegacyVersionedRowMerger(
                std::move(rowBuffer),
                tableSchema->GetColumnCount(),
                tableSchema->GetKeyColumnCount(),
                columnFilter,
                std::move(config),
                currentTimestamp,
                majorTimestamp,
                std::move(columnEvaluator),
                mergeRowsOnFlush,
                useTtlColumn ? tableSchema->GetTtlColumnIndex() : std::nullopt,
                mergeDeletionsOnFlush);

        case ERowMergerType::Watermark: {
            std::optional<TWatermarkRuntimeData> watermarkRuntimeData;

            if (customRuntimeData) {
                try {
                    auto customRuntimeDataMap = ConvertTo<IMapNodePtr>(customRuntimeData);
                    auto watermarkRuntimeDataConfig = customRuntimeDataMap->GetChildValueOrThrow<TWatermarkRuntimeDataConfig>(CustomRuntimeDataWatermarkKey);

                    auto columnSchema = tableSchema->GetColumnOrThrow(watermarkRuntimeDataConfig.ColumnName);
                    if (!columnSchema.IsOfV1Type(ESimpleLogicalValueType::Uint64)) {
                        THROW_ERROR_EXCEPTION(
                            "Unexpected type for watermark column %Qv: expected %Qlv, got %Qlv",
                            columnSchema.Name(),
                            EValueType::Uint64,
                            *columnSchema.LogicalType());
                    }

                    watermarkRuntimeData = TWatermarkRuntimeData{
                        .Watermark = watermarkRuntimeDataConfig.Watermark,
                        .ColumnIndex = tableSchema->GetColumnIndex(watermarkRuntimeDataConfig.ColumnName)
                    };
                } catch (const std::exception& ex) {
                    YT_LOG_ERROR(ex, "Failed to prepare watermark runtime data");
                    watermarkRuntimeData = std::nullopt;
                }
            }

            return std::make_unique<TVersionedRowMerger>(
                std::move(rowBuffer),
                tableSchema->GetColumnCount(),
                tableSchema->GetKeyColumnCount(),
                columnFilter,
                std::move(config),
                currentTimestamp,
                majorTimestamp,
                std::move(columnEvaluator),
                mergeRowsOnFlush,
                useTtlColumn ? tableSchema->GetTtlColumnIndex() : std::nullopt,
                mergeDeletionsOnFlush,
                std::move(watermarkRuntimeData));
        }

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
