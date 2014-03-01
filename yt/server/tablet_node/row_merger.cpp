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
    , MergedValues_(SchemaColumnCount_)
    , ColumnFlags_(SchemaColumnCount_)
{
    if (columnFilter.All) {
        for (int id = 0; id < schemaColumnCount; ++id) {
            ColumnFlags_[id] = true;
            ColumnIds_.push_back(id);
        }
    } else {
        for (int id : columnFilter.Indexes) {
            if (id < 0 || id >= schemaColumnCount) {
                THROW_ERROR_EXCEPTION("Invalid column id %d in column filter",
                    id);
            }
            ColumnFlags_[id] = true;
            ColumnIds_.push_back(id);
        }
    }
}

void TUnversionedRowMerger::Start(const TUnversionedValue* keyBegin)
{
    LatestWrite_ = NullTimestamp;
    LatestDelete_ = NullTimestamp;
    for (int id = 0; id < KeyColumnCount_; ++id) {
        MergedValues_[id] = MakeVersionedValue(keyBegin[id], MaxTimestamp);
    }
    for (int id = KeyColumnCount_; id < SchemaColumnCount_; ++id) {
        MergedValues_[id] = MakeVersionedSentinelValue(EValueType::Null, NullTimestamp, id);
    }
}

void TUnversionedRowMerger::AddPartialRow(TVersionedRow row)
{
    YASSERT(row.GetKeyCount() == KeyColumnCount_);
    YASSERT(row.GetTimestampCount() == 1);

    auto rowTimestamp = row.BeginTimestamps()[0];
    auto rowTimestampValue = rowTimestamp & TimestampValueMask;
    
    // Fast lane.
    if (rowTimestampValue < LatestDelete_)
        return;

    if (rowTimestamp & TombstoneTimestampMask) {
        LatestDelete_ = std::max(LatestDelete_, rowTimestampValue);
    } else {
        LatestWrite_ = std::max(LatestWrite_, rowTimestampValue);

        if (!(rowTimestamp & IncrementalTimestampMask)) {
            LatestDelete_ = std::max(LatestDelete_, rowTimestampValue);
        }

        const auto* rowValues = row.BeginValues();
        for (int index = 0; index < row.GetValueCount(); ++index) {
            const auto& value = rowValues[index];
            int id = value.Id;
            if (ColumnFlags_[id] && MergedValues_[id].Timestamp < value.Timestamp) {
                MergedValues_[id] = value;
            }
        }
    }
}

TUnversionedRow TUnversionedRowMerger::BuildMergedRow()
{
    if (LatestWrite_ == NullTimestamp || LatestWrite_ < LatestDelete_) {
        return TUnversionedRow();
    }

    auto row = TUnversionedRow::Allocate(Pool_, ColumnIds_.size());
    auto* outputValue = row.Begin();
    for (int id : ColumnIds_) {
        auto& mergedValue = MergedValues_[id];
        if (mergedValue.Timestamp < LatestDelete_) {
            mergedValue.Type = EValueType::Null;
        }
        *outputValue++ = mergedValue;
    }

    return row;
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

