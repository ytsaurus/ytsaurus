#include "row_merger.h"
#include "private.h"

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/library/numeric/algorithm_helpers.h>

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
    TTimestamp retentionTimestamp,
    const TTimestampColumnMapping& timestampColumnMapping,
    TNestedColumnsSchema nestedColumnsSchema)
    : RowBuffer_(rowBuffer)
    , KeyColumnCount_(keyColumnCount)
    , ColumnEvaluator_(std::move(columnEvaluator))
    , RetentionTimestamp_(retentionTimestamp)
    , NestedColumnsSchema_(std::move(nestedColumnsSchema))
{
    ColumnIdToTimestampColumnId_.assign(static_cast<size_t>(columnCount), -1);
    IsTimestampColumn_.assign(static_cast<size_t>(columnCount), false);
    for (auto [columnId, timestampColumnId] : timestampColumnMapping) {
        ColumnIdToTimestampColumnId_[columnId] = timestampColumnId;
        IsTimestampColumn_[timestampColumnId] = true;
    }

    if (columnFilter.IsUniversal()) {
        ColumnIds_.resize(columnCount);
        std::iota(ColumnIds_.begin(), ColumnIds_.end(), 0);
    } else {
        ColumnIds_ = columnFilter.GetIndexes();
    }

    bool hasNestedColumns = false;

    ColumnIdToIndex_.assign(static_cast<size_t>(columnCount), -1);
    for (int columnIndex = 0; columnIndex < std::ssize(ColumnIds_); ++columnIndex) {
        auto columnId = ColumnIds_[columnIndex];
        ColumnIdToIndex_[columnId] = columnIndex;

        if (IsTimestampColumn_[columnId]) {
            continue;
        }

        if (const auto* ptr = GetNestedColumnById(NestedColumnsSchema_.KeyColumns, columnId)) {
            AggregateColumnIds_.push_back(columnId);
            hasNestedColumns = true;
            continue;
        }

        if (const auto* ptr = GetNestedColumnById(NestedColumnsSchema_.ValueColumns, columnId)) {
            AggregateColumnIds_.push_back(columnId);
            hasNestedColumns = true;
            continue;
        }

        if (ColumnEvaluator_->IsAggregate(columnId)) {
            AggregateColumnIds_.push_back(columnId);
        }
    }

    if (hasNestedColumns) {
        // Enrich with additional columns.
        for (auto [nestedColumnId, _] : NestedColumnsSchema_.KeyColumns) {
            if (ColumnIdToIndex_[nestedColumnId] == -1) {
                AggregateColumnIds_.push_back(nestedColumnId);
            }
        }
    }

    std::sort(AggregateColumnIds_.begin(), AggregateColumnIds_.end());

    MergedTimestamps_.resize(std::ssize(ColumnIds_));
    AggregateValues_.resize(std::ssize(AggregateColumnIds_));

    Cleanup();
}

void TSchemafulRowMerger::AddPartialRow(TVersionedRow row)
{
    if (!row) {
        return;
    }

    YT_ASSERT(row.GetWriteTimestampCount() <= 1);
    YT_ASSERT(row.GetDeleteTimestampCount() <= 1);

    AddPartialRow(row, MaxTimestamp);
}

void TSchemafulRowMerger::AddPartialRow(TVersionedRow row, TTimestamp upperTimestampLimit)
{
    if (!row) {
        return;
    }

    if (upperTimestampLimit < RetentionTimestamp_) {
        return;
    }

    if (!Started_) {
        if (!MergedRow_) {
            MergedRow_ = RowBuffer_->AllocateUnversioned(ColumnIds_.size());
        }

        for (int columnIndex = 0; columnIndex < std::ssize(ColumnIds_); ++columnIndex) {
            if (int columnId = ColumnIds_[columnIndex]; columnId >= KeyColumnCount_) {
                MergedTimestamps_[columnIndex] = NullTimestamp;
                MergedRow_[columnIndex] = MakeUnversionedNullValue(columnId);
            }
        }

        for (auto key : row.Keys()) {
            if (int columnIndex = ColumnIdToIndex_[key.Id]; columnIndex != -1) {
                MergedTimestamps_[columnIndex] = MaxTimestamp;
                MergedRow_[columnIndex] = key;
            }
        }

        Started_ = true;
    }

    for (auto timestamp : row.DeleteTimestamps()) {
        if (timestamp < upperTimestampLimit && timestamp >= RetentionTimestamp_) {
            LatestDelete_ = std::max(LatestDelete_, timestamp);
            break;
        }
    }

    for (auto timestamp : row.WriteTimestamps()) {
        if (timestamp < upperTimestampLimit && timestamp >= LatestDelete_ && timestamp >= RetentionTimestamp_) {
            LatestWrite_ = std::max(LatestWrite_, timestamp);
            break;
        }
    }

    for (const auto& partialValue : row.Values()) {
        if (partialValue.Timestamp >= upperTimestampLimit) {
            continue;
        }
        if (partialValue.Timestamp > LatestDelete_ && partialValue.Timestamp >= RetentionTimestamp_) {
            int columnId = partialValue.Id;

            auto columnIdIt = LowerBound(AggregateColumnIds_.begin(), AggregateColumnIds_.end(), columnId);
            if (columnIdIt != AggregateColumnIds_.end() && *columnIdIt == columnId) {
                AggregateValues_[columnIdIt - AggregateColumnIds_.begin()].push_back(partialValue);
            } else if (int columnIndex = ColumnIdToIndex_[columnId]; columnIndex >= 0) {
                if (MergedTimestamps_[columnIndex] < partialValue.Timestamp) {
                    MergedRow_[columnIndex] = partialValue;
                    MergedTimestamps_[columnIndex] = partialValue.Timestamp;
                }
            }

            if (int timestampColumnId = ColumnIdToTimestampColumnId_[columnId]; timestampColumnId >= 0) {
                int timestampColumnIndex = ColumnIdToIndex_[timestampColumnId];

                if (partialValue.Timestamp > MergedTimestamps_[timestampColumnIndex]) {
                    MergedRow_[timestampColumnIndex] = MakeUnversionedUint64Value(partialValue.Timestamp, timestampColumnId);
                    MergedTimestamps_[timestampColumnIndex] = partialValue.Timestamp;
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

    for (auto& values : AggregateValues_) {
        values.erase(
            std::remove_if(
                values.begin(),
                values.end(),
                [latestDelete = LatestDelete_] (const TVersionedValue& value) {
                    return value.Timestamp <= latestDelete;
                }),
            values.end());

        std::sort(
            values.begin(),
            values.end(),
            [] (const TVersionedValue& lhs, const TVersionedValue& rhs) {
                return lhs.Timestamp < rhs.Timestamp;
            });

        values.erase(
            std::unique(
                values.begin(),
                values.end(),
                [] (const TVersionedValue& lhs, const TVersionedValue& rhs) {
                    return lhs.Timestamp == rhs.Timestamp;
                }),
            values.end());
    }

    NestedKeyColumns_.assign(std::ssize(NestedColumnsSchema_.KeyColumns), {});
    NestedValueColumns_.assign(std::ssize(NestedColumnsSchema_.ValueColumns), {});

    for (int aggregateIndex = 0; aggregateIndex < std::ssize(AggregateValues_); ++aggregateIndex) {
        auto& values = AggregateValues_[aggregateIndex];
        auto columnId = AggregateColumnIds_[aggregateIndex];

        if (values.empty()) {
            continue;
        }

        auto it = values.begin();
        auto itEnd = values.end();

        for (auto next = it; next != itEnd; ++next) {
            if (None(next->Flags & EValueFlags::Aggregate)) {
                // Skip older aggregate values.
                it = next;
            }
        }

        if (const auto* ptr = GetNestedColumnById(NestedColumnsSchema_.KeyColumns, columnId)) {
            NestedKeyColumns_[ptr - NestedColumnsSchema_.KeyColumns.data()] = {it, itEnd};
            continue;
        }

        if (const auto* ptr = GetNestedColumnById(NestedColumnsSchema_.ValueColumns, columnId)) {
            NestedValueColumns_[ptr - NestedColumnsSchema_.ValueColumns.data()] = {it, itEnd};
            continue;
        }

        auto state = *it++;
        while (it != itEnd) {
            ColumnEvaluator_->MergeAggregate(columnId, &state, *it, RowBuffer_);
            ++it;
        }

        TUnversionedValue finalizedState{};
        ColumnEvaluator_->FinalizeAggregate(columnId, &finalizedState, state, RowBuffer_);

        auto columnIndex = ColumnIdToIndex_[columnId];
        MergedTimestamps_[columnIndex] = (it - 1)->Timestamp;
        MergedRow_[columnIndex] = finalizedState;
    }

    NestedMerger_.UnpackKeyColumns(NestedKeyColumns_, NestedColumnsSchema_.KeyColumns);

    for (int index = 0; index < std::ssize(NestedColumnsSchema_.KeyColumns); ++index) {
        if (NestedKeyColumns_[index].Empty()) {
            continue;
        }

        auto columnId = NestedColumnsSchema_.KeyColumns[index].Id;

        auto state = NestedMerger_.BuildMergedKeyColumns(index, RowBuffer_.Get());

        auto columnIndex = ColumnIdToIndex_[columnId];
        // Nested key columns are added to enriched column filter.
        if (columnIndex != -1) {
            MergedTimestamps_[columnIndex] = NestedKeyColumns_[index].Back().Timestamp;
            MergedRow_[columnIndex] = state;
        }
    }

    for (int index = 0; index < std::ssize(NestedColumnsSchema_.ValueColumns); ++index) {
        auto valueIt = NestedValueColumns_[index].Begin();
        auto endCompactValueIt = NestedValueColumns_[index].End();

        if (NestedValueColumns_[index].Empty()) {
            continue;
        }

        auto columnId = NestedColumnsSchema_.ValueColumns[index].Id;

        auto state = NestedMerger_.BuildMergedValueColumn(
            {valueIt, endCompactValueIt},
            NestedColumnsSchema_.ValueColumns[index].Type,
            NestedColumnsSchema_.ValueColumns[index].AggregateFunction,
            RowBuffer_.Get());

        auto columnIndex = ColumnIdToIndex_[columnId];
        // For nested value columns requested and enriched column filters are matched.
        MergedTimestamps_[columnIndex] = (endCompactValueIt - 1)->Timestamp;
        MergedRow_[columnIndex] = state;
    }

    for (int columnIndex = 0; columnIndex < std::ssize(ColumnIds_); ++columnIndex) {
        int columnId = ColumnIds_[columnIndex];
        bool notAggregate = IsTimestampColumn_[columnId] || !ColumnEvaluator_->IsAggregate(columnId);

        if (MergedTimestamps_[columnIndex] < LatestDelete_ && notAggregate) {
            MergedRow_[columnIndex] = MakeUnversionedNullValue(columnId);
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
    AggregateValues_.resize(std::ssize(AggregateColumnIds_));

    LatestWrite_ = NullTimestamp;
    LatestDelete_ = NullTimestamp;
    Started_ = false;
}

////////////////////////////////////////////////////////////////////////////////

TUnversionedRowMerger::TUnversionedRowMerger(
    TRowBufferPtr rowBuffer,
    int columnCount,
    int keyColumnCount,
    TColumnEvaluatorPtr columnEvaluator,
    TNestedColumnsSchema nestedColumnsSchema)
    : RowBuffer_(std::move(rowBuffer))
    , ColumnCount_(columnCount)
    , KeyColumnCount_(keyColumnCount)
    , ColumnEvaluator_(std::move(columnEvaluator))
    , NestedColumnsSchema_(std::move(nestedColumnsSchema))
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

    NestedKeyColumns_.assign(std::ssize(NestedColumnsSchema_.KeyColumns), {});
    NestedValueColumns_.assign(std::ssize(NestedColumnsSchema_.ValueColumns), {});
    PartialRowCount_ = 0;
}

void TUnversionedRowMerger::AddPartialRow(TUnversionedRow row)
{
    YT_VERIFY(row);

    for (int partialIndex = KeyColumnCount_; partialIndex < static_cast<int>(row.GetCount()); ++partialIndex) {
        const auto& partialValue = row[partialIndex];
        int id = partialValue.Id;
        YT_VERIFY(id >= KeyColumnCount_);

        auto& mergedValue = MergedRow_[id];

        if (Any(partialValue.Flags & EValueFlags::Aggregate)) {
            if (ColumnEvaluator_->IsAggregate(id)) {
                bool isAggregate = Any(mergedValue.Flags & EValueFlags::Aggregate);
                ColumnEvaluator_->MergeAggregate(id, &mergedValue, partialValue, RowBuffer_);
                if (isAggregate) {
                    mergedValue.Flags |= EValueFlags::Aggregate;
                }
            } else {
                // Nested aggregate.
                YT_VERIFY(!ValidValues_[id - KeyColumnCount_]);
                mergedValue = partialValue;
            }
        } else {
            mergedValue = partialValue;
        }
        ValidValues_[id - KeyColumnCount_] = true;
    }

    // Validate nested columns.
    {
        int nestedItemCount;
        bool aggregateFlag;

        bool isFirstColumn = true;

        for (int index = 0; index < std::ssize(NestedColumnsSchema_.KeyColumns); ++index) {
            auto columnId = NestedColumnsSchema_.KeyColumns[index].Id;

            if (!ValidValues_[columnId - KeyColumnCount_]) {
                continue;
            }

            auto& mergedValue = MergedRow_[columnId];

            if (mergedValue.Type == EValueType::Null) {
                ValidValues_[columnId - KeyColumnCount_] = false;
                continue;
            }

            auto currentItemCount = UnpackNestedValuesList(
                nullptr,
                mergedValue.AsStringBuf(),
                NestedColumnsSchema_.KeyColumns[index].Type);

            auto currentAggregateFlag = Any(mergedValue.Flags & EValueFlags::Aggregate);

            if (isFirstColumn) {
                isFirstColumn = false;
                nestedItemCount = currentItemCount;
                aggregateFlag = currentAggregateFlag;

                if (!aggregateFlag) {
                    NestedKeyColumns_.assign(std::ssize(NestedColumnsSchema_.KeyColumns), {});
                    NestedValueColumns_.assign(std::ssize(NestedColumnsSchema_.ValueColumns), {});
                    PartialRowCount_ = 0;
                }
            } else {
                if (nestedItemCount != currentItemCount) {
                    THROW_ERROR_EXCEPTION("Item count mismatch in nested key column")
                        << TErrorAttribute("expected", nestedItemCount)
                        << TErrorAttribute("actual", currentItemCount)
                        << TErrorAttribute("column_id", static_cast<int>(columnId));
                }

                if (aggregateFlag != currentAggregateFlag) {
                    THROW_ERROR_EXCEPTION("Aggregate flag mismatch in nested key column")
                        << TErrorAttribute("expected", aggregateFlag)
                        << TErrorAttribute("actual", currentAggregateFlag)
                        << TErrorAttribute("column_id", static_cast<int>(columnId));
                }
            }

            auto& nestedValues = NestedKeyColumns_[index];

            TVersionedValue value;
            static_cast<TUnversionedValue&>(value) = mergedValue;
            value.Timestamp = PartialRowCount_;
            nestedValues.push_back(value);

            ValidValues_[columnId - KeyColumnCount_] = false;
        }

        for (int index = 0; index < std::ssize(NestedColumnsSchema_.ValueColumns); ++index) {
            auto columnId = NestedColumnsSchema_.ValueColumns[index].Id;

            if (!ValidValues_[columnId - KeyColumnCount_]) {
                continue;
            }

            auto& mergedValue = MergedRow_[columnId];

            if (mergedValue.Type == EValueType::Null) {
                ValidValues_[columnId - KeyColumnCount_] = false;
                continue;
            }

            auto currentItemCount = UnpackNestedValuesList(
                nullptr,
                mergedValue.AsStringBuf(),
                NestedColumnsSchema_.ValueColumns[index].Type);

            auto currentAggregateFlag = Any(mergedValue.Flags & EValueFlags::Aggregate);

            if (isFirstColumn) {
                THROW_ERROR_EXCEPTION("Nested value column occured without nested key column")
                    << TErrorAttribute("column_id", static_cast<int>(columnId));
            }

            if (nestedItemCount != currentItemCount) {
                THROW_ERROR_EXCEPTION("Item count mismatch in nested value column")
                    << TErrorAttribute("expected", nestedItemCount)
                    << TErrorAttribute("actual", currentItemCount)
                    << TErrorAttribute("column_id", static_cast<int>(columnId));
            }

            if (aggregateFlag != Any(mergedValue.Flags & EValueFlags::Aggregate)) {
                THROW_ERROR_EXCEPTION("Aggregate flag mismatch in nested value column")
                    << TErrorAttribute("expected", aggregateFlag)
                    << TErrorAttribute("actual", currentAggregateFlag)
                    << TErrorAttribute("column_id", static_cast<int>(columnId));
            }

            auto& nestedValues = NestedValueColumns_[index];

            TVersionedValue value;
            static_cast<TUnversionedValue&>(value) = mergedValue;
            value.Timestamp = PartialRowCount_;
            nestedValues.push_back(value);

            ValidValues_[columnId - KeyColumnCount_] = false;
        }

        ++PartialRowCount_;
    }
}

void TUnversionedRowMerger::DeletePartialRow(TUnversionedRow /*row*/)
{
    // NB: Since we don't have delete timestamps here we need to write null into all columns.

    for (int index = KeyColumnCount_; index < ColumnCount_; ++index) {
        ValidValues_[index - KeyColumnCount_] = true;
        MergedRow_[index] = MakeUnversionedNullValue(index);
    }

    NestedKeyColumns_.assign(std::ssize(NestedColumnsSchema_.KeyColumns), {});
    NestedValueColumns_.assign(std::ssize(NestedColumnsSchema_.ValueColumns), {});
    PartialRowCount_ = 0;
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
    NestedMerger_.UnpackKeyColumns(NestedKeyColumns_, NestedColumnsSchema_.KeyColumns);

    for (int index = 0; index < std::ssize(NestedColumnsSchema_.KeyColumns); ++index) {
        if (NestedKeyColumns_[index].empty()) {
            continue;
        }
        auto initialAggregateFlags = NestedValueColumns_[index].front().Flags & EValueFlags::Aggregate;

        auto columnId = NestedColumnsSchema_.KeyColumns[index].Id;
        auto state = NestedMerger_.BuildMergedKeyColumns(index, RowBuffer_.Get());

        state.Id = columnId;
        state.Type = EValueType::Composite;
        state.Flags = (state.Flags & ~EValueFlags::Aggregate) | initialAggregateFlags;

        MergedRow_[columnId] = state;
        ValidValues_[columnId - KeyColumnCount_] = true;
    }

    for (int index = 0; index < std::ssize(NestedColumnsSchema_.ValueColumns); ++index) {
        if (NestedValueColumns_[index].empty()) {
            continue;
        }

        auto initialAggregateFlags = NestedValueColumns_[index].front().Flags & EValueFlags::Aggregate;

        auto columnId = NestedColumnsSchema_.ValueColumns[index].Id;

        auto state = NestedMerger_.BuildMergedValueColumn(
            NestedValueColumns_[index],
            NestedColumnsSchema_.ValueColumns[index].Type,
            NestedColumnsSchema_.ValueColumns[index].AggregateFunction,
            RowBuffer_.Get());

        state.Id = columnId;
        state.Type = EValueType::Composite;
        state.Flags = (state.Flags & ~EValueFlags::Aggregate) | initialAggregateFlags;

        MergedRow_[columnId] = state;
        ValidValues_[columnId - KeyColumnCount_] = true;
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

    MergedRow_ = {};
    return mergedRow;
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
        mergedRow[index] = row.Keys()[index];
    }

    for (int index = row.GetKeyCount(); index < SampledColumnCount_; ++index) {
        mergedRow[index] = MakeUnversionedSentinelValue(EValueType::Null, index);
    }

    auto deleteTimestamp = row.GetDeleteTimestampCount() > 0
        ? row.DeleteTimestamps()[0]
        : NullTimestamp;

    for (const auto& value : row.Values()) {
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
