#include "stdafx.h"
#include "row_merger.h"

namespace NYT {
namespace NTabletNode {

using namespace NVersionedTableClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TUnversionedRowMerger::TUnversionedRowMerger(
    TChunkedMemoryPool* pool,
    int schemaColumnCount,
    int keyColumnCount,
    const TColumnFilter& columnFilter)
    : Pool_(pool)
    , SchemaColumnCount_(schemaColumnCount)
    , KeyColumnCount_(keyColumnCount)
    , KeyComparer_(KeyColumnCount_)
{
    if (columnFilter.All) {
        for (int id = 0; id < SchemaColumnCount_; ++id) {
            ColumnIds_.push_back(id);
        }
    } else {
        for (int id : columnFilter.Indexes) {
            if (id < 0 || id >= SchemaColumnCount_) {
                THROW_ERROR_EXCEPTION("Invalid column id %d in column filter",
                    id);
            }
            ColumnIds_.push_back(id);
        }
    }

    ColumnIdToIndex_.resize(SchemaColumnCount_);
    for (int id = 0; id < SchemaColumnCount_; ++id) {
        ColumnIdToIndex_[id] = -1;
    }
    for (int index = 0; index < static_cast<int>(ColumnIds_.size()); ++index) {
        int id = ColumnIds_[index];
        if (id >= KeyColumnCount_) {
            ColumnIdToIndex_[id] = index;            
        }
    }
}

void TUnversionedRowMerger::Start(const TUnversionedValue* keyBegin)
{
    LatestWrite_ = NullTimestamp;
    LatestDelete_ = NullTimestamp;

    if (!MergedRow_) {
        MergedRow_ = TUnversionedRow::Allocate(Pool_, ColumnIds_.size());
    }

    auto* mergedValuesBegin = MergedRow_.Begin();
    auto* mergedTimestampsBegin = MergedTimestamps_.data();
    for (int index = 0; index < static_cast<int>(ColumnIds_.size()); ++index) {
        int id = ColumnIds_[index];
        auto* mergedValue = mergedValuesBegin + index;
        mergedValue->Id = id;
        if (id < KeyColumnCount_) {
            *mergedValue = keyBegin[id];
            mergedTimestampsBegin[index] = MaxTimestamp;
        } else {
            mergedValue->Type = EValueType::Null;
            mergedTimestampsBegin[index] = NullTimestamp;
        }
    }
}

void TUnversionedRowMerger::AddPartialRow(TVersionedRow row)
{
    YASSERT(row.GetKeyCount() == KeyColumnCount_);
    YASSERT(row.GetTimestampCount() == 1);

    auto rowTimestamp = row.BeginTimestamps()[0];
    auto rowTimestampValue = rowTimestamp & TimestampValueMask;
    
    if (rowTimestampValue < LatestDelete_)
        return;

    if (rowTimestamp & TombstoneTimestampMask) {
        LatestDelete_ = std::max(LatestDelete_, rowTimestampValue);
    } else {
        LatestWrite_ = std::max(LatestWrite_, rowTimestampValue);

        if (!(rowTimestamp & IncrementalTimestampMask)) {
            LatestDelete_ = std::max(LatestDelete_, rowTimestampValue);
        }

        const auto* partialValuesBegin = row.BeginValues();
        auto* mergedValuesBegin = MergedRow_.Begin();
        auto* mergedTimestampsBegin = MergedTimestamps_.data();
        for (int partialIndex = 0; partialIndex < row.GetValueCount(); ++partialIndex) {
            const auto& partialValue = partialValuesBegin[partialIndex];
            int id = partialValue.Id;
            int mergedIndex = ColumnIdToIndex_[id];
            if (mergedIndex >= 0 && mergedTimestampsBegin[mergedIndex] < partialValue.Timestamp) {
                mergedValuesBegin[mergedIndex] = partialValue;
                mergedTimestampsBegin[mergedIndex] = partialValue.Timestamp;
            }
        }
    }
}

TUnversionedRow TUnversionedRowMerger::BuildMergedRow()
{
    if (LatestWrite_ == NullTimestamp || LatestWrite_ < LatestDelete_) {
        return TUnversionedRow();
    }

    const auto* mergedTimestampsBegin = MergedTimestamps_.data();
    auto* mergedValuesBegin = MergedRow_.Begin();
    for (int index = 0; index < static_cast<int>(ColumnIds_.size()); ++index) {
        if (mergedTimestampsBegin[index] < LatestDelete_) {
            mergedValuesBegin[index].Type = EValueType::Null;
        }
    }

    auto mergedRow = MergedRow_;
    MergedRow_ = TUnversionedRow();
    return mergedRow;
}

////////////////////////////////////////////////////////////////////////////////

TVersionedRowMerger::TVersionedRowMerger(
    TChunkedMemoryPool* pool,
    int schemaColumnCount,
    int keyColumnCount)
    : Pool_(pool)
    , SchemaColumnCount_(schemaColumnCount)
    , KeyColumnCount_(keyColumnCount)
    , KeyComparer_(KeyColumnCount_)
    , Keys_(KeyColumnCount_)
{ }

void TVersionedRowMerger::Start(const TUnversionedValue* keyBegin)
{
    std::copy(keyBegin, keyBegin + KeyColumnCount_, Keys_.data());
    Values_.clear();
    Timestamps_.clear();
}

void TVersionedRowMerger::AddPartialRow(TVersionedRow row)
{
    Values_.insert(Values_.end(), row.BeginValues(), row.EndValues());
    Timestamps_.insert(Timestamps_.end(), row.BeginTimestamps(), row.EndTimestamps());
}

TVersionedRow TVersionedRowMerger::BuildMergedRow()
{
    std::sort(
        Values_.begin(),
        Values_.end(),
        [] (const TVersionedValue& lhs, const TVersionedValue& rhs) -> bool {
            if (lhs.Id < rhs.Id) {
                return true;
            }
            if (lhs.Id > rhs.Id) {
                return false;
            }
            if (lhs.Timestamp > rhs.Timestamp) {
                return true;
            }
            if (lhs.Timestamp < rhs.Timestamp) {
                return false;
            }
            return false;
        });

    MergedValues_.clear();
    for (const auto& currentValue : Values_) {
        if (!MergedValues_.empty()) {
            const auto& prevValue = MergedValues_.back();
            if (currentValue.Id == prevValue.Id && currentValue.Timestamp == prevValue.Timestamp) {
                YCHECK(CompareRowValues(currentValue, prevValue) == 0);
                continue;
            }
        }
        MergedValues_.push_back(currentValue);
    }

    std::sort(
        Timestamps_.begin(),
        Timestamps_.end(),
        [] (TTimestamp lhs, TTimestamp rhs) {
            return (lhs & TimestampValueMask) < (rhs & TimestampValueMask);
        });

    MergedTimestamps_.clear();
    for (auto timestamp : Timestamps_) {
        auto currentTimestamp = timestamp & ~IncrementalTimestampMask;
        YASSERT((currentTimestamp & TimestampValueMask) != UncommittedTimestamp);
        if (MergedTimestamps_.empty()) {
            if (!(currentTimestamp & TombstoneTimestampMask)) {
                currentTimestamp |= IncrementalTimestampMask;
            }
        } else {
            auto prevTimestamp = MergedTimestamps_.back();
            if ((prevTimestamp & TimestampValueMask) == (currentTimestamp & TimestampValueMask)) {
                YCHECK((prevTimestamp & TombstoneTimestampMask) == (currentTimestamp & TombstoneTimestampMask));
                continue;
            }
            if ((prevTimestamp & TombstoneTimestampMask) == (currentTimestamp & TombstoneTimestampMask))
                continue;
        }
        MergedTimestamps_.push_back(currentTimestamp);
    }

    auto row = TVersionedRow::Allocate(Pool_, KeyColumnCount_, MergedValues_.size(), MergedTimestamps_.size());
    std::copy(Keys_.begin(), Keys_.end(), row.BeginKeys());
    std::copy(MergedValues_.begin(), MergedValues_.end(), row.BeginValues());
    std::copy(MergedTimestamps_.begin(), MergedTimestamps_.end(), row.BeginTimestamps());
    return row;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

