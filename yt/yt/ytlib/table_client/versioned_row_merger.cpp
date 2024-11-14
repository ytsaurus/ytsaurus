#include "versioned_row_merger.h"
#include "nested_row_merger.h"

#include "config.h"

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/unversioned_value.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/library/numeric/algorithm_helpers.h>

namespace NYT::NTableClient {

using namespace NQueryClient;
using namespace NTransactionClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class T>
static i64 GetVectorBytesCapacity(const std::vector<T>& vector)
{
    return vector.capacity() * sizeof(T);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TLegacyVersionedRowMerger
    : public IVersionedRowMerger
{
public:
    TLegacyVersionedRowMerger(
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
        IMemoryUsageTrackerPtr memoryTracker)
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
        , MemoryTrackerGuard_(TMemoryUsageTrackerGuard::Build(std::move(memoryTracker)))
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

        RecalculateMemoryUsage();
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

            auto retentionBeginIt = ColumnValues_.begin();

            // Apply retention config if present.
            if (Config_) {
                YT_VERIFY(rowMaxDataTtl);
                retentionBeginIt = ColumnValues_.end();

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

        RecalculateMemoryUsage();
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

    TMemoryUsageTrackerGuard MemoryTrackerGuard_;

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

        RecalculateMemoryUsage();
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

    void RecalculateMemoryUsage()
    {
        if (MemoryTrackerGuard_) {
            i64 memoryUsage = RowBuffer_->GetCapacity() +
                GetVectorBytesCapacity(PartialValues_) +
                GetVectorBytesCapacity(ColumnValues_) +
                GetVectorBytesCapacity(MergedValues_) +
                GetVectorBytesCapacity(WriteTimestamps_) +
                GetVectorBytesCapacity(DeleteTimestamps_);

            MemoryTrackerGuard_.TrySetSize(memoryUsage)
                .ThrowOnError();
        }
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
    bool mergeDeletionsOnFlush,
    IMemoryUsageTrackerPtr memoryTracker)
{
    return std::make_unique<TLegacyVersionedRowMerger>(
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
        std::move(memoryTracker));
}

////////////////////////////////////////////////////////////////////////////////

class TNewVersionedRowMerger
    : public IVersionedRowMerger
{
public:
    TNewVersionedRowMerger(
        TRowBufferPtr rowBuffer,
        NQueryClient::TColumnEvaluatorPtr columnEvaluator,
        int columnCount,
        int keyColumnCount,
        const TColumnFilter& columnFilter,
        // Keep all versions not older (newer or equal to) than all versiones timestamp.
        TTimestamp allVersionsTimestamp,
        // Remove versions older than retention timestamp.
        TTimestamp retentionTimestamp,
        // There are no timestamps older than major timestamp in other stores.
        // Compact aggregate values older than major timestamp timestamp.
        TTimestamp majorTimestamp,
        // Timestamp range of all rows are exclusive to current merge session.
        // FlushMode assumes that timestamp range in store is not intersected with any other for given key range.
        // Adjacent delete timestamps and aggregates (before allVersionsTimestamp) can be merged.
        // Major timestamp should be zero in flush mode.
        bool flushMode,
        TNestedColumnsSchema nestedColumnsSchema)
        : RowBuffer_(std::move(rowBuffer))
        , ColumnEvaluator_(std::move(columnEvaluator))
        , AllVersionsTimestamp_(allVersionsTimestamp)
        , RetentionTimestamp_(retentionTimestamp)
        , MajorTimestamp_(majorTimestamp)
        , FlushMode_(flushMode)
        , NestedColumnsSchema_(std::move(nestedColumnsSchema))
    {
        YT_VERIFY(RetentionTimestamp_ <= AllVersionsTimestamp_);
        YT_VERIFY(MajorTimestamp_ <= AllVersionsTimestamp_);

        // Flush mode cannot be replaced with MajorTimestamp == Max.
        // In case of MajorTimestamp all delete timestamps before MajorTimestamp can be removed.
        // In case of FlushMode last delete timesamp before AllVersionsTimestamp must be kept
        // because there can be writes before it in chunks.

        int resultKeyColumnCount = 0;
        int resultValueColumnCount = 0;

        ColumnIdToIndex_.resize(columnCount, -1);

        int columnIndex = 0;
        auto addColumn = [&] (int id) {
            ColumnIdToIndex_[id] = columnIndex++;

            if (id < keyColumnCount) {
                ++resultKeyColumnCount;
            } else {
                ++resultValueColumnCount;
            }
        };

        if (columnFilter.IsUniversal()) {
            for (int id = 0; id < columnCount; ++id) {
                addColumn(id);
            }
        } else {
            for (int id : columnFilter.GetIndexes()) {
                addColumn(id);
            }
        }

        Keys_.resize(resultKeyColumnCount);
        Values_.resize(resultValueColumnCount);

        for (int columnId = keyColumnCount; columnId < columnCount; ++columnId) {
            if (ColumnIdToIndex_[columnId] == -1) {
                continue;
            }

            YT_VERIFY(ColumnIdToIndex_[columnId] >= resultKeyColumnCount);
            int valueIndex = ColumnIdToIndex_[columnId] - resultKeyColumnCount;

            // TODO(lukyan): Support column filter for nested columns.

            if (GetNestedColumnById(NestedColumnsSchema_.KeyColumns, columnId)) {
                continue;
            }

            if (GetNestedColumnById(NestedColumnsSchema_.ValueColumns, columnId)) {
                continue;
            }

            if (ColumnEvaluator_->IsAggregate(columnId)) {
                AggregateColumnIndexes_.push_back(valueIndex);
            } else {
                ReplacingColumnIndexes_.push_back(valueIndex);
            }
        }
    }

    void AddPartialRow(TVersionedRow row, TTimestamp upperTimestampLimit) override
    {
        if (!row) {
            return;
        }

        if (!Started_) {
            Started_ = true;

            for (const auto& key : row.Keys()) {
                YT_VERIFY(key.Id < ColumnIdToIndex_.size());
                auto index = ColumnIdToIndex_[key.Id];
                if (index != -1) {
                    YT_VERIFY(index >= 0 && index < std::ssize(Keys_));
                    Keys_[index] = key;
                }
            }
        }

        for (const auto& value : row.Values()) {
            YT_VERIFY(value.Id < ColumnIdToIndex_.size());
            auto index = ColumnIdToIndex_[value.Id];

            if (value.Timestamp >= upperTimestampLimit) {
                continue;
            }

            if (index != -1) {
                YT_VERIFY(index >= std::ssize(Keys_));
                YT_VERIFY(index - std::ssize(Keys_) < std::ssize(Values_));
                Values_[index - std::ssize(Keys_)].push_back(value);
            } else {
                // Save timestamps of filtered out columns.
                WriteTimestamps_.push_back(value.Timestamp);
            }
        }

        for (auto timestamp : row.WriteTimestamps()) {
            if (timestamp < upperTimestampLimit) {
                WriteTimestamps_.push_back(timestamp);
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

        auto baseDeleteTimestamp = GetBaseDeleteTimestamp();
        auto retentionTimestamp = std::max(RetentionTimestamp_, baseDeleteTimestamp);

        // Filter write timestamps of filtered out columns.
        WriteTimestamps_.erase(
            std::remove_if(WriteTimestamps_.begin(), WriteTimestamps_.end(), [&] (auto timestamp) {
                return Precedes(timestamp, retentionTimestamp);
            }),
            WriteTimestamps_.end());

        std::sort(WriteTimestamps_.begin(), WriteTimestamps_.end());

        // Apply retention config for write timestamps of filtered out columns.
        {
            auto endOneVersionTimestampIt = LowerBound(WriteTimestamps_.begin(), WriteTimestamps_.end(), AllVersionsTimestamp_);

            if (WriteTimestamps_.begin() < endOneVersionTimestampIt) {
                --endOneVersionTimestampIt;
                WriteTimestamps_.erase(WriteTimestamps_.begin(), endOneVersionTimestampIt);
            }
        }

        for (auto columnIndex : ReplacingColumnIndexes_) {
            auto& values = Values_[columnIndex];
            auto [valueIt, valueItEnd] = SortUniqueValuesAndSkipDeletedPrefix(&values, baseDeleteTimestamp);

            auto endOneVersionValueIt = valueIt;

            while (endOneVersionValueIt < valueItEnd &&
                Precedes(endOneVersionValueIt->Timestamp, AllVersionsTimestamp_))
            {
                ++endOneVersionValueIt;
            }

            if (valueIt < endOneVersionValueIt) {
                valueIt = endOneVersionValueIt - 1;
                // Apply retention in the end symmetrically with logic for aggregate columns.
                if (Precedes(valueIt->Timestamp, RetentionTimestamp_)) {
                    ++valueIt;
                }
            }

            values.erase(values.begin(), valueIt);
        }

        auto compactAggregatesTimestamp = FlushMode_ ? AllVersionsTimestamp_ : MajorTimestamp_;

        auto getAggregationRange = [&] (TValuesRange values) -> TValuesRange {
            auto [valueIt, valueItEnd] = values;

            auto endCompactValueIt = valueIt;

            // Determine merged values range (common with nested merge).
            while (endCompactValueIt < valueItEnd &&
                Precedes(endCompactValueIt->Timestamp, compactAggregatesTimestamp))
            {
                // Override non-delta.
                if (None(endCompactValueIt->Flags & EValueFlags::Aggregate)) {
                    valueIt = endCompactValueIt;
                }

                ++endCompactValueIt;
            }

            return {valueIt, endCompactValueIt};
        };

        for (auto columnIndex : AggregateColumnIndexes_) {
            auto& values = Values_[columnIndex];
            auto valuesRange = SortUniqueValuesAndSkipDeletedPrefix(&values, baseDeleteTimestamp);
            auto [valueIt, endCompactValueIt] = getAggregationRange(valuesRange);

            if (valueIt < endCompactValueIt) {
                auto state = ApplyAggregation(valueIt, endCompactValueIt);
                valueIt = endCompactValueIt - 1;
                *valueIt = state;

                // FIXME(lukyan): Fix case when major timestamp is lower then retention timestamp.
                // Consider retention timestamp equal to major timestamp minus max data TTL for aggregate columns.

                // Apply retention timestamp after aggregation to make result stable regardless of compaction
                // period (no compaction for a long time or it was performed many times).
                if (Precedes(valueIt->Timestamp, RetentionTimestamp_)) {
                    ++valueIt;
                }
            }

            values.erase(values.begin(), valueIt);
        }

        NestedKeyColumns_.clear();

        auto resultKeyColumnCount = std::ssize(Keys_);

        for (auto [nestedKeyColumnId, _] : NestedColumnsSchema_.KeyColumns) {
            YT_VERIFY(ColumnIdToIndex_[nestedKeyColumnId] >= resultKeyColumnCount);
            int columnIndex = ColumnIdToIndex_[nestedKeyColumnId] - resultKeyColumnCount;

            auto& values = Values_[columnIndex];
            auto valuesRange = SortUniqueValuesAndSkipDeletedPrefix(&values, baseDeleteTimestamp);
            auto [valueIt, endCompactValueIt] = getAggregationRange(valuesRange);

            NestedKeyColumns_.push_back({valueIt, endCompactValueIt});
        }

        NestedMerger_.UnpackKeyColumns(NestedKeyColumns_, NestedColumnsSchema_.KeyColumns);

        for (int index = 0; index < std::ssize(NestedColumnsSchema_.KeyColumns); ++index) {
            auto nestedKeyColumnId = NestedColumnsSchema_.KeyColumns[index].Id;
            YT_VERIFY(ColumnIdToIndex_[nestedKeyColumnId] >= resultKeyColumnCount);
            int columnIndex = ColumnIdToIndex_[nestedKeyColumnId] - resultKeyColumnCount;

            auto valueIt = NestedKeyColumns_[index].Begin();
            auto endCompactValueIt = NestedKeyColumns_[index].End();

            if (valueIt < endCompactValueIt) {
                auto initialAggregateFlags = valueIt->Flags & EValueFlags::Aggregate;

                auto state = NestedMerger_.BuildMergedKeyColumns(index, RowBuffer_.Get());

                // TODO(lukyan): Move inside BuildMergedKeyColumns?
                state.Id = nestedKeyColumnId;
                state.Flags = (state.Flags & ~EValueFlags::Aggregate) | initialAggregateFlags;

                valueIt = endCompactValueIt - 1;
                *valueIt = state;

                if (Precedes(valueIt->Timestamp, RetentionTimestamp_)) {
                    ++valueIt;
                }
            }

            auto& values = Values_[columnIndex];

            values.erase(values.begin(), valueIt);
        }

        for (int index = 0; index < std::ssize(NestedColumnsSchema_.ValueColumns); ++index) {
            auto nestedValueColumnId = NestedColumnsSchema_.ValueColumns[index].Id;
            YT_VERIFY(ColumnIdToIndex_[nestedValueColumnId] >= resultKeyColumnCount);
            int columnIndex = ColumnIdToIndex_[nestedValueColumnId] - resultKeyColumnCount;

            auto& values = Values_[columnIndex];
            auto valuesRange = SortUniqueValuesAndSkipDeletedPrefix(&values, baseDeleteTimestamp);
            auto [valueIt, endCompactValueIt] = getAggregationRange(valuesRange);

            if (valueIt < endCompactValueIt) {
                auto initialAggregateFlags = valueIt->Flags & EValueFlags::Aggregate;

                auto mergedValue = NestedMerger_.BuildMergedValueColumn(
                    {valueIt, endCompactValueIt},
                    NestedColumnsSchema_.ValueColumns[index].Type,
                    NestedColumnsSchema_.ValueColumns[index].AggregateFunction,
                    RowBuffer_.Get());

                mergedValue.Id = nestedValueColumnId;
                mergedValue.Flags = (mergedValue.Flags & ~EValueFlags::Aggregate) | initialAggregateFlags;

                valueIt = endCompactValueIt - 1;
                *valueIt = mergedValue;

                if (Precedes(valueIt->Timestamp, RetentionTimestamp_)) {
                    ++valueIt;
                }
            }

            values.erase(values.begin(), valueIt);
        }

        int resultValueCount = 0;
        for (int index = 0; index < std::ssize(Values_); ++index) {
            auto& values = Values_[index];

            for (int j = 1; j < std::ssize(values); ++j) {
                YT_VERIFY(Precedes(values[j - 1].Timestamp, values[j].Timestamp));
            }

            // Reverse timestamps.
            std::reverse(values.begin(), values.end());

            resultValueCount += std::ssize(values);

            // Save output values timestamps.
            for (const auto& value : values) {
                WriteTimestamps_.push_back(value.Timestamp);
            }
        }

        std::sort(WriteTimestamps_.begin(), WriteTimestamps_.end());

        WriteTimestamps_.erase(
            std::unique(WriteTimestamps_.begin(), WriteTimestamps_.end()),
            WriteTimestamps_.end());

        // Remove redundant delete timestamps between subsequent write timestamps.
        if (FlushMode_) {
            RemoveDeletesBetweenSubsequentWrites();
        }

        if (!produceEmptyRow &&
            resultValueCount == 0 &&
            WriteTimestamps_.empty() &&
            DeleteTimestamps_.empty())
        {
            Cleanup();
            return {};
        }

        // Reverse write and delete timestamps list to make them appear in descending order.
        std::reverse(WriteTimestamps_.begin(), WriteTimestamps_.end());
        std::reverse(DeleteTimestamps_.begin(), DeleteTimestamps_.end());

        // Construct output row.
        auto row = RowBuffer_->AllocateVersioned(
            Keys_.size(),
            resultValueCount,
            WriteTimestamps_.size(),
            DeleteTimestamps_.size());

        // Construct output keys.
        std::copy(Keys_.begin(), Keys_.end(), row.BeginKeys());

        auto valuesIt = row.BeginValues();
        for (int index = 0; index < std::ssize(Values_); ++index) {
            valuesIt = std::copy(Values_[index].begin(), Values_[index].end(), valuesIt);
        }

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
    using TValuesRange = std::pair<TVersionedValue*, TVersionedValue*>;

    const TRowBufferPtr RowBuffer_;
    const NQueryClient::TColumnEvaluatorPtr ColumnEvaluator_;

    const TTimestamp AllVersionsTimestamp_;
    const TTimestamp RetentionTimestamp_;

    // Major timestamp is upper exclusive timestamp for merge session.
    // There are no timestamps older than this in other stores (not in current merge session).
    const TTimestamp MajorTimestamp_;
    const bool FlushMode_;
    const TNestedColumnsSchema NestedColumnsSchema_;

    std::vector<int> ColumnIdToIndex_;
    std::vector<TUnversionedValue> Keys_;

    std::vector<std::vector<TVersionedValue>> Values_;
    std::vector<int> ReplacingColumnIndexes_;
    std::vector<int> AggregateColumnIndexes_;

    std::vector<TTimestamp> WriteTimestamps_;
    std::vector<TTimestamp> DeleteTimestamps_;

    TNestedTableMerger NestedMerger_;
    std::vector<TMutableRange<TVersionedValue>> NestedKeyColumns_;

    bool Started_ = false;

    void Cleanup()
    {
        for (int index = 0; index < std::ssize(Values_); ++index) {
            Values_[index].clear();
        }

        WriteTimestamps_.clear();
        DeleteTimestamps_.clear();

        Started_ = false;
    }

    // Use separate function to force particular relation operator.
    // Separate function also useful to mark all timestamp comparisons.
    static bool Precedes(TTimestamp a, TTimestamp b)
    {
        return a < b;
    }

    // TODO(lukyan): Move calculations to AddPartialRow.
    TTimestamp GetBaseDeleteTimestamp()
    {
        // Sort delete timestamps in ascending order and remove duplicates.
        std::sort(DeleteTimestamps_.begin(), DeleteTimestamps_.end());
        DeleteTimestamps_.erase(
            std::unique(DeleteTimestamps_.begin(), DeleteTimestamps_.end()),
            DeleteTimestamps_.end());

        auto deleteTimestampIt = DeleteTimestamps_.begin();
        // Depends on condition for Retention timestamp.
        while (deleteTimestampIt != DeleteTimestamps_.end() &&
            Precedes(*deleteTimestampIt, AllVersionsTimestamp_))
        {
            ++deleteTimestampIt;
        }

        TTimestamp baseDeleteTimestamp = NullTimestamp;

        if (deleteTimestampIt > DeleteTimestamps_.begin()) {
            baseDeleteTimestamp = deleteTimestampIt[-1];

            // Preserve last delete timestamp preceding all versions timestamp
            // if it is greater than major timestamp.
            // Timestamp must be preserved because of older writes in other chunks.
            if (!Precedes(baseDeleteTimestamp, MajorTimestamp_)) {
                --deleteTimestampIt;
            } else {
                YT_VERIFY(Precedes(baseDeleteTimestamp, AllVersionsTimestamp_));
                YT_VERIFY(Precedes(baseDeleteTimestamp, MajorTimestamp_));
            }

            DeleteTimestamps_.erase(DeleteTimestamps_.begin(), deleteTimestampIt);
        }

        return baseDeleteTimestamp;
    }

    void RemoveDeletesBetweenSubsequentWrites()
    {
        auto nextWriteTimestampIt = WriteTimestamps_.begin();
        auto deleteTimestampOutputIt = DeleteTimestamps_.begin();
        bool deleteTimestampStored = false;

        for (auto deleteTimestamp : DeleteTimestamps_) {
            while (nextWriteTimestampIt != WriteTimestamps_.end() && !Precedes(deleteTimestamp, *nextWriteTimestampIt)) {
                deleteTimestampStored = false;
                nextWriteTimestampIt++;
            }

            if (!deleteTimestampStored) {
                *deleteTimestampOutputIt++ = deleteTimestamp;
                deleteTimestampStored = true;
            }
        }

        DeleteTimestamps_.erase(deleteTimestampOutputIt, DeleteTimestamps_.end());
    }

    static TValuesRange SortUniqueValuesAndSkipDeletedPrefix(std::vector<TVersionedValue>* values, TTimestamp baseDeleteTimestamp)
    {
        auto valueIt = values->data();
        auto valueItEnd = valueIt + values->size();

        std::sort(
            valueIt,
            valueItEnd,
            [] (const TVersionedValue& lhs, const TVersionedValue& rhs) {
                return Precedes(lhs.Timestamp, rhs.Timestamp);
            });

        valueItEnd = std::unique(
            valueIt,
            valueItEnd,
            [] (const TVersionedValue& lhs, const TVersionedValue& rhs) {
                return lhs.Timestamp == rhs.Timestamp;
            });

        values->erase(valueItEnd, values->end());

        // Skip versions before base delete timestamp.
        while (valueIt != valueItEnd && Precedes(valueIt->Timestamp, baseDeleteTimestamp)) {
            ++valueIt;
        }

        return {valueIt, valueItEnd};
    }

    TVersionedValue ApplyAggregation(TVersionedValue* valueIt, TVersionedValue* valueItEnd)
    {
        auto state = *valueIt++;
        auto id = state.Id;
        // Aggregation result gets last timestamp.
        state.Timestamp = valueItEnd[-1].Timestamp;

        // The very first aggregated value determines the final aggregation mode.
        // Preserve initial aggregate flag.
        auto initialAggregateFlags = state.Flags & EValueFlags::Aggregate;

        for (; valueIt < valueItEnd; ++valueIt) {
            const auto& value = *valueIt;

            // Do no expect any tombstones.
            YT_ASSERT(value.Type != EValueType::TheBottom);
            // Overwrites are expected at the very beginning only.
            YT_ASSERT(Any(value.Flags & EValueFlags::Aggregate));

            ColumnEvaluator_->MergeAggregate(id, &state, value, RowBuffer_);

            // Preserve aggregate flag in aggregate functions.
            state.Flags &= ~EValueFlags::Aggregate;
            state.Flags |= initialAggregateFlags;
        }

        // Value is not finalized yet. Further merges may happen.
        YT_ASSERT((state.Flags & EValueFlags::Aggregate) == initialAggregateFlags);

        return state;
    }
};

std::pair<TTimestamp, TTimestamp> GetPivotTimestamps(TTimestamp currentTimestamp, TRetentionConfigPtr config)
{
    if (!config) {
        return {0, 0};
    }

    auto minTtl = config->MinDataTtl.Seconds() << NTransactionClient::TimestampCounterWidth;
    auto maxTtl = config->MaxDataTtl.Seconds() << NTransactionClient::TimestampCounterWidth;

    auto allVersionsTimestamp = currentTimestamp > minTtl ? currentTimestamp - minTtl : 0;

    TTimestamp retentionTimestamp;
    if (config->MinDataVersions > 0) {
        retentionTimestamp = 0;
    } else {
        // Consider config->MaxDataTtl as config->MaxDataTtl + config->MinDataTtl to guarantee stable
        // repeatable read.
        maxTtl += minTtl;

        retentionTimestamp = currentTimestamp > maxTtl ? currentTimestamp - maxTtl : 0;
    }

    return {retentionTimestamp, allVersionsTimestamp};
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedRowMerger> CreateNewVersionedRowMerger(
    TRowBufferPtr rowBuffer,
    int columnCount,
    int keyColumnCount,
    const TColumnFilter& columnFilter,
    TRetentionConfigPtr config,
    TTimestamp currentTimestamp,
    TTimestamp majorTimestamp,
    NQueryClient::TColumnEvaluatorPtr columnEvaluator,
    bool mergeRowsOnFlush,
    TNestedColumnsSchema nestedColumnsSchema)
{
    if (config && config->IgnoreMajorTimestamp) {
        mergeRowsOnFlush = true;
    }

    // Lookup mode is redundant because behaviour can be achieved with universal retention config.
    // Universal retention config is one with infinity min data TTL.

    auto [retentionTimestamp, allVersionsTimestamp] = GetPivotTimestamps(currentTimestamp, config);
    return std::make_unique<TNewVersionedRowMerger>(
        rowBuffer,
        columnEvaluator,
        columnCount,
        keyColumnCount,
        columnFilter,
        allVersionsTimestamp,
        retentionTimestamp,
        std::min(majorTimestamp, allVersionsTimestamp),
        mergeRowsOnFlush,
        nestedColumnsSchema);
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
    bool mergeRowsOnFlush,
    bool useTtlColumn,
    bool mergeDeletionsOnFlush,
    IMemoryUsageTrackerPtr memoryTracker)
{
    if (useTtlColumn && tableSchema->GetTtlColumnIndex()) {
        rowMergerType = ERowMergerType::Legacy;
    }

    auto nestedColumnsSchema = GetNestedColumnsSchema(tableSchema);

    if (!nestedColumnsSchema.KeyColumns.empty() && rowMergerType != ERowMergerType::New) {
        THROW_ERROR_EXCEPTION("Nested columns are supported only in new versioned row merger");
    }

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
                mergeDeletionsOnFlush,
                std::move(memoryTracker));

        case ERowMergerType::New:
            return CreateNewVersionedRowMerger(
                rowBuffer,
                tableSchema->GetColumnCount(),
                tableSchema->GetKeyColumnCount(),
                columnFilter,
                config,
                currentTimestamp,
                majorTimestamp,
                columnEvaluator,
                mergeRowsOnFlush,
                std::move(nestedColumnsSchema));

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
