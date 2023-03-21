#include "row_merger.h"
#include "config.h"

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYT::NTableClient {

using namespace NTransactionClient;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

TSchemafulRowMerger::TSchemafulRowMerger(
    TRowBufferPtr rowBuffer,
    int columnCount,
    int keyColumnCount,
    const TColumnFilter& columnFilter,
    TColumnEvaluatorPtr columnEvaluator,
    TTimestamp retentionTimestamp)
    : RowBuffer_(rowBuffer)
    , ColumnCount_(columnCount)
    , KeyColumnCount_(keyColumnCount)
    , ColumnEvaluator_(std::move(columnEvaluator))
    , RetentionTimestamp_(retentionTimestamp)
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

    YT_ASSERT(row.GetKeyCount() == KeyColumnCount_);
    YT_ASSERT(row.GetWriteTimestampCount() <= 1);
    YT_ASSERT(row.GetDeleteTimestampCount() <= 1);

    if (!Started_) {
        if (!MergedRow_) {
            MergedRow_ = RowBuffer_->AllocateUnversioned(ColumnIds_.size());
        }

        const auto* keyBegin = row.BeginKeys();
        for (int index = 0; index < static_cast<int>(ColumnIds_.size()); ++index) {
            int id = ColumnIds_[index];
            auto& mergedValue = MergedRow_[index];
            if (id < KeyColumnCount_) {
                MergedTimestamps_[index] = MaxTimestamp;
                mergedValue = keyBegin[id];
            } else {
                MergedTimestamps_[index] = NullTimestamp;
                mergedValue = MakeUnversionedNullValue(id);
             }
        }

        Started_ = true;
    }

    if (row.GetDeleteTimestampCount() > 0) {
        auto deleteTimestamp = row.BeginDeleteTimestamps()[0];
        if (deleteTimestamp >= RetentionTimestamp_) {
            LatestDelete_ = std::max(LatestDelete_, deleteTimestamp);
        }
    }

    if (row.GetWriteTimestampCount() > 0) {
        auto writeTimestamp = row.BeginWriteTimestamps()[0];

        if (writeTimestamp < LatestDelete_ || writeTimestamp < RetentionTimestamp_) {
            return;
        }

        LatestWrite_ = std::max(LatestWrite_, writeTimestamp);

        const auto* partialValuesBegin = row.BeginValues();
        for (int partialIndex = 0; partialIndex < row.GetValueCount(); ++partialIndex) {
            const auto& partialValue = partialValuesBegin[partialIndex];
            if (partialValue.Timestamp > LatestDelete_ && partialValue.Timestamp >= RetentionTimestamp_) {
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

void TSchemafulRowMerger::AddPartialRow(TVersionedRow row, TTimestamp upperTimestampLimit)
{
    if (!row) {
        return;
    }

    if (upperTimestampLimit < RetentionTimestamp_) {
        return;
    }

    Y_ASSERT(row.GetKeyCount() == KeyColumnCount_);

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
                *mergedValue = MakeUnversionedNullValue(id);
             }
        }

        Started_ = true;
    }

    for (auto it = row.BeginDeleteTimestamps(); it != row.EndDeleteTimestamps(); ++it) {
        if (*it < upperTimestampLimit && *it >= RetentionTimestamp_) {
            LatestDelete_ = std::max(LatestDelete_, *it);
            break;
        }
    }

    for (auto it = row.BeginWriteTimestamps(); it != row.EndWriteTimestamps(); ++it) {
        if (*it < upperTimestampLimit && *it >= RetentionTimestamp_) {
            LatestWrite_ = std::max(LatestWrite_, *it);
            break;
        }
    }

    for (auto it = row.BeginValues(); it != row.EndValues(); ++it) {
        const auto& partialValue = *it;
        if (partialValue.Timestamp >= upperTimestampLimit) {
            continue;
        }
        if (partialValue.Timestamp > LatestDelete_ && partialValue.Timestamp >= RetentionTimestamp_) {
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

TMutableUnversionedRow TSchemafulRowMerger::BuildMergedRow()
{
    if (!Started_) {
        return {};
    }

    if (LatestWrite_ == NullTimestamp || LatestWrite_ < LatestDelete_) {
        Cleanup();
        return {};
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

    for (auto it = AggregateValues_.begin(), end = AggregateValues_.end(); it != end;) {
        int id = it->Id;

        // Find first element with different id.
        auto next = it;
        while (++next != end && id == next->Id) {
            if (None(next->Flags & EValueFlags::Aggregate)) {
                // Skip older aggregate values.
                it = next;
            }
        }

        auto state = *it++;
        while (it != next) {
            ColumnEvaluator_->MergeAggregate(id, &state, *it, RowBuffer_);
            ++it;
        }

        ColumnEvaluator_->FinalizeAggregate(id, &state, state, RowBuffer_);

#if 0
        state.Aggregate = false;
#endif

        auto columnIndex = ColumnIdToIndex_[id];
        MergedTimestamps_[columnIndex] = (it - 1)->Timestamp;
        MergedRow_[columnIndex] = state;
    }

    for (int index = 0; index < static_cast<int>(ColumnIds_.size()); ++index) {
        int id = ColumnIds_[index];
        if (MergedTimestamps_[index] < LatestDelete_ && !ColumnEvaluator_->IsAggregate(id)) {
            MergedRow_[index] = MakeUnversionedNullValue(index);
        }
    }

    auto mergedRow = MergedRow_;

    Cleanup();
    return mergedRow;
}

void TSchemafulRowMerger::Reset()
{
    YT_ASSERT(!Started_);
    RowBuffer_->Clear();
    MergedRow_ = {};
}

void TSchemafulRowMerger::Cleanup()
{
    MergedRow_ = {};
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
    : RowBuffer_(std::move(rowBuffer))
    , ColumnCount_(columnCount)
    , KeyColumnCount_(keyColumnCount)
    , ColumnEvaluator_(std::move(columnEvaluator))
    , ValidValues_(size_t(ColumnCount_) - KeyColumnCount_, false)
{
    YT_VERIFY(KeyColumnCount_ <= ColumnCount_);
}

void TUnversionedRowMerger::InitPartialRow(TUnversionedRow row)
{
    MergedRow_ = RowBuffer_->AllocateUnversioned(ColumnCount_);

    ValidValues_.assign(ColumnCount_ - KeyColumnCount_, false);

    std::copy(row.begin(), row.begin() + KeyColumnCount_, MergedRow_.begin());

    for (int index = KeyColumnCount_; index < ColumnCount_; ++index) {
        auto flags = EValueFlags::None;
        if (ColumnEvaluator_->IsAggregate(index)) {
            flags |= EValueFlags::Aggregate;
        }
        MergedRow_[index] = MakeUnversionedNullValue(index, flags);
    }
}

void TUnversionedRowMerger::AddPartialRow(TUnversionedRow row)
{
    YT_VERIFY(row);

    for (int partialIndex = KeyColumnCount_; partialIndex < static_cast<int>(row.GetCount()); ++partialIndex) {
        const auto& partialValue = row[partialIndex];
        int id = partialValue.Id;
        YT_VERIFY(id >= KeyColumnCount_);
        ValidValues_[id - KeyColumnCount_] = true;
        auto& mergedValue = MergedRow_[id];
        if (Any(partialValue.Flags & EValueFlags::Aggregate)) {
            YT_VERIFY(ColumnEvaluator_->IsAggregate(id));
            bool isAggregate = Any(mergedValue.Flags & EValueFlags::Aggregate);
            ColumnEvaluator_->MergeAggregate(id, &mergedValue, partialValue, RowBuffer_);
            if (isAggregate) {
                mergedValue.Flags |= EValueFlags::Aggregate;
            }
        } else {
            mergedValue = partialValue;
        }
    }
}

void TUnversionedRowMerger::DeletePartialRow(TUnversionedRow /*row*/)
{
    // NB: Since we don't have delete timestamps here we need to write null into all columns.

    for (int index = KeyColumnCount_; index < ColumnCount_; ++index) {
        ValidValues_[index - KeyColumnCount_] = true;
        MergedRow_[index] = MakeUnversionedNullValue(index);
    }
}

TMutableUnversionedRow TUnversionedRowMerger::BuildDeleteRow()
{
    auto mergedRow = MergedRow_;
    mergedRow.SetCount(KeyColumnCount_);
    MergedRow_ = {};
    return mergedRow;
}

TMutableUnversionedRow TUnversionedRowMerger::BuildMergedRow()
{
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

        TUnversionedValue* it = MergedRow_.begin() + KeyColumnCount_;
        auto jt = std::copy(MergedRow_.begin(), it, mergedRow.begin());

        YT_VERIFY(static_cast<int>(MergedRow_.GetCount()) == ColumnCount_);
        for (bool isValid : ValidValues_) {
            if (isValid) {
                *jt++ = *it;
            }
            ++it;
        }

        mergedRow.SetCount(jt - mergedRow.begin());
    }

    MergedRow_ = TMutableUnversionedRow();
    return mergedRow;
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

void TVersionedRowMerger::AddPartialRow(TVersionedRow row, TTimestamp upperTimestampLimit)
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
                Keys_.data()[index] = row.BeginKeys()[id];
            }
        }
    }

    for (auto it = row.BeginValues(); it != row.EndValues(); ++it) {
        if (it->Timestamp < upperTimestampLimit) {
            PartialValues_.push_back(*it);
        }
    }

    for (auto it = row.BeginDeleteTimestamps(); it != row.EndDeleteTimestamps(); ++it) {
        if (*it < upperTimestampLimit) {
            DeleteTimestamps_.push_back(*it);
        }
    }
}

TMutableVersionedRow TVersionedRowMerger::BuildMergedRow()
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
            while (retentionBeginIt != ColumnValues_.begin()
                && retentionBeginIt->Timestamp >= MajorTimestamp_
                && !MergeRowsOnFlush_)
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

void TVersionedRowMerger::Reset()
{
    YT_ASSERT(!Started_);
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
    TTableSchemaPtr schema)
    : RowBuffer_(std::move(rowBuffer))
    , KeyColumnCount_(schema->GetKeyColumnCount())
    , LatestTimestamps_(static_cast<size_t>(schema->GetColumnCount()), NullTimestamp)
    , IdMapping_(static_cast<size_t>(schema->GetColumnCount()), -1)
{
    for (const auto& column : schema->Columns()) {
        if (!column.Aggregate()) {
            IdMapping_[schema->GetColumnIndex(column)] = SampledColumnCount_;
            ++SampledColumnCount_;
        }
    }
}

TMutableUnversionedRow TSamplingRowMerger::MergeRow(TVersionedRow row)
{
    auto mergedRow = RowBuffer_->AllocateUnversioned(SampledColumnCount_);

    YT_VERIFY(row.GetKeyCount() == KeyColumnCount_);
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

} // namespace NYT::NTableClient

