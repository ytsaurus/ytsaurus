#include "row_merger.h"
#include "config.h"

#include <yt/ytlib/query_client/column_evaluator.h>

#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/client/table_client/row_buffer.h>

namespace NYT {
namespace NTableClient {

using namespace NTransactionClient;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

TSchemafulRowMerger::TSchemafulRowMerger(
    TRowBufferPtr rowBuffer,
    int columnCount,
    int keyColumnCount,
    const TColumnFilter& columnFilter,
    TColumnEvaluatorPtr columnEvaluator)
    : RowBuffer_(rowBuffer)
    , ColumnCount_(columnCount)
    , KeyColumnCount_(keyColumnCount)
    , ColumnEvaluator_(std::move(columnEvaluator))
{
    if (columnFilter.IsUniversal()) {
        for (int id = 0; id < ColumnCount_; ++id) {
            ColumnIds_.push_back(id);
        }
    } else {
        for (int id : columnFilter.GetIndexes()) {
            ColumnIds_.push_back(id);
        }
    }

    ColumnIdToIndex_.resize(ColumnCount_);
    for (int id = 0; id < ColumnCount_; ++id) {
        ColumnIdToIndex_[id] = -1;
    }
    for (int index = 0; index < static_cast<int>(ColumnIds_.size()); ++index) {
        int id = ColumnIds_[index];
        if (id >= KeyColumnCount_) {
            ColumnIdToIndex_[id] = index;
        }
    }

    MergedTimestamps_.resize(ColumnCount_);

    Cleanup();
}

void TSchemafulRowMerger::AddPartialRow(TVersionedRow row)
{
    if (!row) {
        return;
    }

    Y_ASSERT(row.GetKeyCount() == KeyColumnCount_);
    Y_ASSERT(row.GetWriteTimestampCount() <= 1);
    Y_ASSERT(row.GetDeleteTimestampCount() <= 1);

    if (!Started_) {
        if (!MergedRow_) {
            MergedRow_ = RowBuffer_->AllocateUnversioned(ColumnIds_.size());
        }

        const auto* keyBegin = row.BeginKeys();
        for (int index = 0; index < static_cast<int>(ColumnIds_.size()); ++index) {
            int id = ColumnIds_[index];
            auto* mergedValue = &MergedRow_[index];
            if (id < KeyColumnCount_) {
                MergedTimestamps_[index] = MaxTimestamp;
                *mergedValue = keyBegin[id];
            } else {
                MergedTimestamps_[index] = NullTimestamp;
                mergedValue->Id = id;
                mergedValue->Type = EValueType::Null;
                mergedValue->Aggregate = false;
             }
        }

        Started_ = true;
    }

    if (row.GetDeleteTimestampCount() > 0) {
        auto deleteTimestamp = row.BeginDeleteTimestamps()[0];
        LatestDelete_ = std::max(LatestDelete_, deleteTimestamp);
    }

    if (row.GetWriteTimestampCount() > 0) {
        auto writeTimestamp = row.BeginWriteTimestamps()[0];
        LatestWrite_ = std::max(LatestWrite_, writeTimestamp);

        if (writeTimestamp < LatestDelete_) {
            return;
        }

        const auto* partialValuesBegin = row.BeginValues();
        for (int partialIndex = 0; partialIndex < row.GetValueCount(); ++partialIndex) {
            const auto& partialValue = partialValuesBegin[partialIndex];
            if (partialValue.Timestamp > LatestDelete_) {
                int id = partialValue.Id;
                int mergedIndex = ColumnIdToIndex_[id];
                if (mergedIndex >= 0) {
                    if (ColumnEvaluator_->IsAggregate(id)) {
                        AggregateValues_.push_back(partialValue);
                    } else if (MergedTimestamps_[mergedIndex] < partialValue.Timestamp) {
                        MergedRow_[mergedIndex] = partialValue;
                        MergedTimestamps_[mergedIndex] = partialValue.Timestamp;
                    }
                }
            }
        }
    }
}

TUnversionedRow TSchemafulRowMerger::BuildMergedRow()
{
    if (!Started_) {
        return TUnversionedRow();
    }

    if (LatestWrite_ == NullTimestamp || LatestWrite_ < LatestDelete_) {
        Cleanup();
        return TUnversionedRow();
    }

    AggregateValues_.erase(
        std::remove_if(
            AggregateValues_.begin(),
            AggregateValues_.end(),
            [latestDelete = LatestDelete_] (const TVersionedValue& value) {
                return value.Timestamp <= latestDelete;
            }),
        AggregateValues_.end());

    std::sort(
        AggregateValues_.begin(),
        AggregateValues_.end(),
        [] (const TVersionedValue& lhs, const TVersionedValue& rhs) {
            return std::tie(lhs.Id, lhs.Timestamp) < std::tie(rhs.Id, rhs.Timestamp);
        });

    AggregateValues_.erase(
        std::unique(
            AggregateValues_.begin(),
            AggregateValues_.end(),
            [] (const TVersionedValue& lhs, const TVersionedValue& rhs) {
                return std::tie(lhs.Id, lhs.Timestamp) == std::tie(rhs.Id, rhs.Timestamp);
            }),
        AggregateValues_.end());

    int begin = 0;
    for (int index = 0; index < AggregateValues_.size(); ++index) {
        if (index == AggregateValues_.size() - 1 || AggregateValues_[begin].Id != AggregateValues_[index + 1].Id) {
            int id = AggregateValues_[begin].Id;
            auto state = MakeUnversionedSentinelValue(EValueType::Null, id);

            for (int valueIndex = index; valueIndex >= begin; --valueIndex) {
                if (!AggregateValues_[valueIndex].Aggregate) {
                    begin = valueIndex;
                }
            }

            for (int valueIndex = begin; valueIndex <= index; ++valueIndex) {
                ColumnEvaluator_->MergeAggregate(id, &state, AggregateValues_[valueIndex], RowBuffer_);
            }

            state.Aggregate = false;
            auto columnIndex = ColumnIdToIndex_[id];
            MergedTimestamps_[columnIndex] = AggregateValues_[index].Timestamp;
            MergedRow_[columnIndex] = state;
            begin = index + 1;
        }
    }

    for (int index = 0; index < static_cast<int>(ColumnIds_.size()); ++index) {
        int id = ColumnIds_[index];
        if (MergedTimestamps_[index] < LatestDelete_ && !ColumnEvaluator_->IsAggregate(id)) {
            MergedRow_[index].Type = EValueType::Null;
        }
    }

    auto mergedRow = MergedRow_;

    Cleanup();
    return mergedRow;
}

void TSchemafulRowMerger::Reset()
{
    Y_ASSERT(!Started_);
    RowBuffer_->Clear();
    MergedRow_ = TMutableUnversionedRow();
}

void TSchemafulRowMerger::Cleanup()
{
    MergedRow_ = TMutableUnversionedRow();
    AggregateValues_.clear();
    LatestWrite_ = NullTimestamp;
    LatestDelete_ = NullTimestamp;
    Started_ = false;
}

////////////////////////////////////////////////////////////////////////////////

TUnversionedRowMerger::TUnversionedRowMerger(
    TRowBufferPtr rowBuffer,
    int columnCount,
    int keyColumnCount,
    TColumnEvaluatorPtr columnEvaluator)
    : RowBuffer_(rowBuffer)
    , ColumnCount_(columnCount)
    , KeyColumnCount_(keyColumnCount)
    , ColumnEvaluator_(std::move(columnEvaluator))
    , ValidValues_(size_t(ColumnCount_), false)
{
    Cleanup();
}

void TUnversionedRowMerger::InitPartialRow(TUnversionedRow row)
{
    if (!Started_) {
        MergedRow_ = RowBuffer_->AllocateUnversioned(ColumnCount_);

        for (int index = 0; index < ColumnCount_; ++index) {
            if (index < KeyColumnCount_) {
                ValidValues_[index] = true;
                MergedRow_[index] = row[index];
            } else {
                ValidValues_[index] = false;
                MergedRow_[index].Id = index;
                MergedRow_[index].Type = EValueType::Null;
                MergedRow_[index].Aggregate = ColumnEvaluator_->IsAggregate(index);
            }
        }
    }

    Started_ = true;
}

void TUnversionedRowMerger::AddPartialRow(TUnversionedRow row)
{
    if (!row) {
        return;
    }

    InitPartialRow(row);

    for (int partialIndex = KeyColumnCount_; partialIndex < row.GetCount(); ++partialIndex) {
        const auto& partialValue = row[partialIndex];
        int id = partialValue.Id;
        ValidValues_[id] = true;

        if (partialValue.Aggregate) {
            YCHECK(ColumnEvaluator_->IsAggregate(id));

            bool isAggregate = MergedRow_[id].Aggregate;
            ColumnEvaluator_->MergeAggregate(id, &MergedRow_[id], partialValue, RowBuffer_);
            MergedRow_[id].Aggregate = isAggregate;
        } else {
            MergedRow_[id] = partialValue;
        }
    }

    Deleted_ = false;
}

void TUnversionedRowMerger::DeletePartialRow(TUnversionedRow row)
{
    // NB: Since we don't have delete timestamps here we need to write null into all columns.

    InitPartialRow(row);

    for (int index = KeyColumnCount_; index < ColumnCount_; ++index) {
        ValidValues_[index] = true;
        MergedRow_[index].Type = EValueType::Null;
        MergedRow_[index].Aggregate = false;
    }

    Deleted_ = true;
}

TUnversionedRow TUnversionedRowMerger::BuildMergedRow()
{
    if (!Started_) {
        return TUnversionedRow();
    }

    if (Deleted_) {
        auto mergedRow = MergedRow_;
        mergedRow.SetCount(KeyColumnCount_);
        Cleanup();
        return mergedRow;
    }

    bool fullRow = true;
    for (bool validValue : ValidValues_) {
        if (!validValue) {
            fullRow = false;
            break;
        }
    }

    TMutableUnversionedRow mergedRow;

    if (fullRow) {
        mergedRow = MergedRow_;
    } else {
        mergedRow = RowBuffer_->AllocateUnversioned(ColumnCount_);
        int currentIndex = 0;
        for (int index = 0; index < MergedRow_.GetCount(); ++index) {
            if (ValidValues_[index]) {
                mergedRow[currentIndex] = MergedRow_[index];
                ++currentIndex;
            }
        }
        mergedRow.SetCount(currentIndex);
    }

    Cleanup();
    return mergedRow;
}

void TUnversionedRowMerger::Reset()
{
    Y_ASSERT(!Started_);
    RowBuffer_->Clear();
    MergedRow_ = TMutableUnversionedRow();
}

void TUnversionedRowMerger::Cleanup()
{
    MergedRow_ = TMutableUnversionedRow();
    Started_ = false;
}

////////////////////////////////////////////////////////////////////////////////

TVersionedRowMerger::TVersionedRowMerger(
    TRowBufferPtr rowBuffer,
    int columnCount,
    int keyColumnCount,
    const TColumnFilter& columnFilter,
    TRetentionConfigPtr config,
    TTimestamp currentTimestamp,
    TTimestamp majorTimestamp,
    TColumnEvaluatorPtr columnEvaluator,
    bool lookup,
    bool mergeRowsOnFlush)
    : RowBuffer_(std::move(rowBuffer))
    , KeyColumnCount_(keyColumnCount)
    , Config_(std::move(config))
    , IgnoreMajorTimestamp_(Config_ ? Config_->IgnoreMajorTimestamp : false)
    , CurrentTimestamp_(currentTimestamp)
    , MajorTimestamp_(IgnoreMajorTimestamp_ ? MaxTimestamp : majorTimestamp)
    , ColumnEvaluator_(std::move(columnEvaluator))
    , Lookup_(lookup)
    , MergeRowsOnFlush_(mergeRowsOnFlush)
{
    size_t mergedKeyColumnCount = 0;
    if (columnFilter.IsUniversal()) {
        for (int id = 0; id < columnCount; ++id) {
            if (id < keyColumnCount) {
                ++mergedKeyColumnCount;
            }
            ColumnIds_.push_back(id);
        }
        Y_ASSERT(mergedKeyColumnCount == keyColumnCount);
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

void TVersionedRowMerger::AddPartialRow(TVersionedRow row)
{
    if (!row) {
        return;
    }

    if (!Started_) {
        Started_ = true;
        Y_ASSERT(row.GetKeyCount() == KeyColumnCount_);
        for (int index = 0; index < ColumnIds_.size(); ++index) {
            int id = ColumnIds_[index];
            if (id < KeyColumnCount_) {
                Y_ASSERT(index < Keys_.size());
                Keys_.data()[index] = row.BeginKeys()[id];
            }
        }
    }

    PartialValues_.insert(
        PartialValues_.end(),
        row.BeginValues(),
        row.EndValues());

    DeleteTimestamps_.insert(
        DeleteTimestamps_.end(),
        row.BeginDeleteTimestamps(),
        row.EndDeleteTimestamps());
}

TVersionedRow TVersionedRowMerger::BuildMergedRow()
{
    if (!Started_) {
        return TVersionedRow();
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
            Y_ASSERT(it->Timestamp <= (it + 1)->Timestamp);
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
            while (retentionBeginIt != ColumnValues_.begin()
                && retentionBeginIt->Timestamp >= MajorTimestamp_
                && !MergeRowsOnFlush_)
            {
                --retentionBeginIt;
            }

            if (retentionBeginIt > ColumnValues_.begin()) {
                auto aggregateBeginIt = ColumnValues_.begin();
                for (auto valueIt = retentionBeginIt; valueIt >= aggregateBeginIt; --valueIt) {
                    if (valueIt->Type == EValueType::TheBottom) {
                        aggregateBeginIt = valueIt + 1;
                        break;
                    }
                    if (!valueIt->Aggregate) {
                        aggregateBeginIt = valueIt;
                        break;
                    }
                }

                if (aggregateBeginIt < retentionBeginIt) {
                    TVersionedValue state;
                    ColumnEvaluator_->InitAggregate(id, &state, RowBuffer_);

                    for (auto valueIt = aggregateBeginIt; valueIt <= retentionBeginIt; ++valueIt) {
                        const auto& value = *valueIt;
                        // Do no expect any thombstones.
                        Y_ASSERT(value.Type != EValueType::TheBottom);
                        // Only expect overwrites at the very beginning.
                        Y_ASSERT(value.Aggregate || valueIt == aggregateBeginIt);
                        ColumnEvaluator_->MergeAggregate(id, &state, value, RowBuffer_);
                    }

                    TUnversionedValue& targetValue = *retentionBeginIt;
                    ColumnEvaluator_->FinalizeAggregate(id, &targetValue, state, RowBuffer_);

                    // The very first aggregated value determines the final aggregation mode.
                    targetValue.Aggregate = aggregateBeginIt->Aggregate;
                }
            }

            if (IgnoreMajorTimestamp_) {
                retentionBeginIt->Aggregate = true;
            } else if (retentionBeginIt->Timestamp < MajorTimestamp_) {
                retentionBeginIt->Aggregate = false;
            }
        }

        // Save output values and timestamps.
        for (auto it = ColumnValues_.rbegin(); it.base() != retentionBeginIt; ++it) {
            const auto& value = *it;
            if (value.Type != EValueType::TheBottom) {
                WriteTimestamps_.push_back(value.Timestamp);
                if (needToSaveColumn) {
                    MergedValues_.push_back(value);
                }
            }
        }

        partialValueIt = columnEndIt;
    }

    // Reverse delete timestamps list to make them appear in descending order.
    std::reverse(DeleteTimestamps_.begin(), DeleteTimestamps_.end());

    // Sort write timestamps in descending order, remove duplicates.
    std::sort(WriteTimestamps_.begin(), WriteTimestamps_.end(), std::greater<TTimestamp>());
    WriteTimestamps_.erase(
        std::unique(WriteTimestamps_.begin(), WriteTimestamps_.end()),
        WriteTimestamps_.end());

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
        return TVersionedRow();
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

void TVersionedRowMerger::Reset()
{
    Y_ASSERT(!Started_);
    RowBuffer_->Clear();
}

void TVersionedRowMerger::Cleanup()
{
    PartialValues_.clear();
    MergedValues_.clear();
    ColumnValues_.clear();

    WriteTimestamps_.clear();
    DeleteTimestamps_.clear();

    Started_ = false;
}

////////////////////////////////////////////////////////////////////////////////

TSamplingRowMerger::TSamplingRowMerger(
    TRowBufferPtr rowBuffer,
    const TTableSchema& schema)
    : RowBuffer_(std::move(rowBuffer))
    , KeyColumnCount_(schema.GetKeyColumnCount())
    , LatestTimestamps_(size_t(schema.GetColumnCount()), NullTimestamp)
    , IdMapping_(size_t(schema.GetColumnCount()), -1)
{
    for (const auto& column : schema.Columns()) {
        if (!column.Aggregate()) {
            IdMapping_[schema.GetColumnIndex(column)] = SampledColumnCount_;
            ++SampledColumnCount_;
        }
    }
}

TUnversionedRow TSamplingRowMerger::MergeRow(TVersionedRow row)
{
    auto mergedRow = RowBuffer_->AllocateUnversioned(SampledColumnCount_);

    YCHECK(row.GetKeyCount() == KeyColumnCount_);
    for (int index = 0; index < row.GetKeyCount(); ++index) {
        mergedRow[index] = row.BeginKeys()[index];
    }

    for (int index = row.GetKeyCount(); index < SampledColumnCount_; ++index) {
        mergedRow[index] = MakeUnversionedSentinelValue(EValueType::Null, index);
    }

    auto deleteTimestamp = row.GetDeleteTimestampCount() > 0
        ? row.BeginDeleteTimestamps()[0]
        : NullTimestamp;

    for (int index = 0; index < row.GetValueCount(); ++index) {
        const auto& value = row.BeginValues()[index];

        if (value.Timestamp < deleteTimestamp || value.Timestamp < LatestTimestamps_[value.Id]) {
            continue;
        }

        auto id = IdMapping_[value.Id];
        if (id != -1) {
            mergedRow[id] = value;
            LatestTimestamps_[id] = value.Timestamp;
        }
    }

    return mergedRow;
}

void TSamplingRowMerger::Reset()
{
    RowBuffer_->Clear();
    std::fill(LatestTimestamps_.begin(), LatestTimestamps_.end(), NullTimestamp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

