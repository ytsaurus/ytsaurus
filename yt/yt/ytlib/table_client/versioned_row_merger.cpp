#include "versioned_row_merger.h"

#include "config.h"

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/client/table_client/unversioned_value.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/client/transaction_client/helpers.h>

namespace NYT::NTableClient {

using namespace NQueryClient;
using namespace NTransactionClient;
using namespace NTabletClient;

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
        bool lookup,
        bool mergeRowsOnFlush,
        bool mergeDeletionsOnFlush)
        : RowBuffer_(std::move(rowBuffer))
        , KeyColumnCount_(keyColumnCount)
        , Config_(std::move(config))
        , IgnoreMajorTimestamp_(Config_ ? Config_->IgnoreMajorTimestamp : false)
        , CurrentTimestamp_(currentTimestamp)
        , MajorTimestamp_(IgnoreMajorTimestamp_ ? MaxTimestamp : majorTimestamp)
        , ColumnEvaluator_(std::move(columnEvaluator))
        , Lookup_(lookup)
        , MergeRowsOnFlush_(mergeRowsOnFlush)
        , MergeDeletionsOnFlush_(mergeDeletionsOnFlush)
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

    TMutableVersionedRow BuildMergedRow() override
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
                        TimestampDiffToDuration(timestamp, CurrentTimestamp_).first > Config_->MaxDataTtl)
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

        if (!Lookup_ && MergedValues_.empty() && WriteTimestamps_.empty() && DeleteTimestamps_.empty()) {
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
    const bool Lookup_ = true;
    const bool MergeRowsOnFlush_;
    const bool MergeDeletionsOnFlush_;

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
    bool lookup,
    bool mergeRowsOnFlush,
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
        lookup,
        mergeRowsOnFlush,
        mergeDeletionsOnFlush);
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedRowMerger> CreateVersionedRowMerger(
    ERowMergerType rowMergerType,
    TRowBufferPtr rowBuffer,
    int columnCount,
    int keyColumnCount,
    const TColumnFilter& columnFilter,
    TRetentionConfigPtr config,
    TTimestamp currentTimestamp,
    TTimestamp majorTimestamp,
    TColumnEvaluatorPtr columnEvaluator,
    bool lookup,
    bool mergeRowsOnFlush,
    bool mergeDeletionsOnFlush)

{
    switch (rowMergerType) {
        case ERowMergerType::Legacy:
            return CreateLegacyVersionedRowMerger(
                rowBuffer,
                columnCount,
                keyColumnCount,
                columnFilter,
                config,
                currentTimestamp,
                majorTimestamp,
                columnEvaluator,
                lookup,
                mergeRowsOnFlush,
                mergeDeletionsOnFlush);
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
