#include "stdafx.h"
#include "row_merger.h"
#include "config.h"

#include <ytlib/transaction_client/helpers.h>

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

    Reset();
}

void TUnversionedRowMerger::AddPartialRow(TVersionedRow row)
{
    YASSERT(row.GetKeyCount() == KeyColumnCount_);
    YASSERT(row.GetTimestampCount() == 1);

    if (!Started_) {
        if (!MergedRow_) {
            MergedRow_ = TUnversionedRow::Allocate(Pool_, ColumnIds_.size());
        }

        const auto* keyBegin = row.BeginKeys();
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

        Started_ = true;
    }

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
    YCHECK(Started_);

    if (LatestWrite_ == NullTimestamp || LatestWrite_ < LatestDelete_) {
        Reset();
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

    Reset();
    return mergedRow;
}

void TUnversionedRowMerger::Reset()
{
    LatestWrite_ = NullTimestamp;
    LatestDelete_ = NullTimestamp;
    Started_ = false;
}

////////////////////////////////////////////////////////////////////////////////

TVersionedRowMerger::TVersionedRowMerger(
    TChunkedMemoryPool* pool,
    int keyColumnCount,
    TRetentionConfigPtr config,
    TTimestamp currentTimestamp,
    TTimestamp majorTimestamp)
    : Pool_(pool)
    , KeyColumnCount_(keyColumnCount)
    , Config_(std::move(config))
    , CurrentTimestamp_(currentTimestamp)
    , MajorTimestamp_(majorTimestamp)
    , KeyComparer_(KeyColumnCount_)
    , Keys_(KeyColumnCount_)
{
    Reset();
}

void TVersionedRowMerger::AddPartialRow(TVersionedRow row)
{
    if (!Started_) {
        Started_ = true;
        YASSERT(row.GetKeyCount() == KeyColumnCount_);
        std::copy(row.BeginKeys(), row.EndKeys(), Keys_.data());
    }

    PartialValues_.insert(PartialValues_.end(), row.BeginValues(), row.EndValues());

    // TODO(babenko): refactor this
    for (auto it = row.BeginTimestamps(); it != row.EndTimestamps(); ++it) {
        auto timestamp = *it;
        if (timestamp & TombstoneTimestampMask) {
            DeleteTimestamps_.push_back(timestamp & TimestampValueMask);
        }
    }
}

TVersionedRow TVersionedRowMerger::BuildMergedRow()
{
    YCHECK(Started_);

    // Clear everything.
    MergedValues_.clear();
    MergedTimestamps_.clear();

    // Sort input values by (id, timestamp).
    std::sort(
        PartialValues_.begin(),
        PartialValues_.end(),
        [] (const TVersionedValue& lhs, const TVersionedValue& rhs) -> bool {
            if (lhs.Id < rhs.Id) {
                return true;
            }
            if (lhs.Id > rhs.Id) {
                return false;
            }
            if (lhs.Timestamp < rhs.Timestamp) {
                return true;
            }
            if (lhs.Timestamp > rhs.Timestamp) {
                return false;
            }
            return false;
        });

    // Scan through input values.
    auto partialValueIt = PartialValues_.begin();
    while (partialValueIt != PartialValues_.end()) {
        // Extract a range of values for the current column.
        ColumnValues_.clear();
        auto columnBeginIt = partialValueIt;
        auto columnEndIt = partialValueIt;
        while (columnEndIt != PartialValues_.end() && columnEndIt->Id == partialValueIt->Id) {
            ++columnEndIt;
        }

        // Merge with delete timestamps and put result into ColumnValues_.
        // Delete timestamps are represented by null sentinels.
        {
            auto timestampBeginIt = DeleteTimestamps_.begin();
            auto timestampEndIt = DeleteTimestamps_.end();
            ColumnValues_.clear();
            auto columnValueIt = columnBeginIt;
            auto timestampIt = timestampBeginIt;
            while (columnValueIt != columnEndIt || timestampIt != timestampEndIt) {
                if (timestampIt == timestampEndIt ||
                    columnValueIt != columnEndIt && columnValueIt->Timestamp < *timestampIt)
                {
                    ColumnValues_.push_back(*columnValueIt++);
                } else {
                    TVersionedValue value;
                    value.Timestamp = (*timestampIt++) | TombstoneTimestampMask;
                    value.Type = EValueType::Null;
                    ColumnValues_.push_back(value);
                } 
            }
        }

#ifndef NDEBUG
        // Validate merged list.
        for (auto it = ColumnValues_.begin(); it != ColumnValues_.end(); ++it) {
            YASSERT(
                it + 1 == ColumnValues_.end() ||
                (it->Timestamp & TimestampValueMask) <= ((it + 1)->Timestamp & TimestampValueMask));
        }
#endif

        // Compute safety limit by min_versions.
        auto safetyEndIt = ColumnValues_.begin();
        if (ColumnValues_.size() > Config_->MinDataVersions) {
            safetyEndIt = ColumnValues_.end() - Config_->MinDataVersions;
        }

        // Adjust safety limit by min_ttl.
        while (safetyEndIt != ColumnValues_.begin()) {
            auto timestamp = (safetyEndIt - 1)->Timestamp & TimestampValueMask;
            if (timestamp < CurrentTimestamp_ &&
                TimestampDiffToDuration(timestamp, CurrentTimestamp_).first > Config_->MinDataTtl)
            {
                break;
            }
            --safetyEndIt;
        }

        // Compute retention limit by max_versions and max_ttl.
        auto retentionBeginIt = safetyEndIt;
        while (retentionBeginIt != ColumnValues_.begin()) {
            if (std::distance(retentionBeginIt, ColumnValues_.end()) >= Config_->MaxDataVersions)
                break;

            auto timestamp = (retentionBeginIt - 1)->Timestamp & TimestampValueMask;
            if (timestamp < CurrentTimestamp_ &&
                TimestampDiffToDuration(timestamp, CurrentTimestamp_).first > Config_->MaxDataTtl)
                break;

            --retentionBeginIt;
        }

        // Save output values and timestamps.
        for (auto it = retentionBeginIt; it != ColumnValues_.end(); ++it) {
            const auto& value = *it;
            MergedTimestamps_.push_back(value.Timestamp);
            if (!(value.Timestamp & TombstoneTimestampMask)) {
                MergedValues_.push_back(*it);
            }
        }

        partialValueIt = columnEndIt;
    }

    // Sort MergedTimestamps_, remove duplicates.
    std::sort(
        MergedTimestamps_.begin(),
        MergedTimestamps_.end(),
        [] (TTimestamp lhs, TTimestamp rhs) {
            return (lhs & TimestampValueMask) < (rhs & TimestampValueMask);
        });

    // Delete redundant tombstones preceding major timestamp.
    // Delete duplicate timestamps.
    {
        // NB: This facilitates dropping the leading tombstones.
        auto prevTimestamp = TombstoneTimestampMask;
        auto it = MergedTimestamps_.begin();
        auto jt = it;
        while (it != MergedTimestamps_.end()) {
            auto timestamp = *it;

            bool keep = true;
            
            if ((timestamp & TimestampValueMask) < MajorTimestamp_ &&
                (timestamp & TombstoneTimestampMask) &&
                (prevTimestamp & TombstoneTimestampMask))
            {
                keep = false;
            }

            if (timestamp == prevTimestamp) {
                keep = false;
            }

            if (keep) {
                *jt++ = prevTimestamp = timestamp;
            }

            ++it;
        }
        MergedTimestamps_.erase(jt, MergedTimestamps_.end());
    }

    if (MergedValues_.empty() && MergedTimestamps_.empty()) {
        Reset();
        return TVersionedRow();
    }

    // TODO(babenko): get rid of this
    if (!(MergedTimestamps_.front() & TombstoneTimestampMask)) {
        MergedTimestamps_.front() |= IncrementalTimestampMask;
    }

    // Construct output row.
    auto row = TVersionedRow::Allocate(Pool_, KeyColumnCount_, MergedValues_.size(), MergedTimestamps_.size());

    // Construct output keys.
    std::copy(Keys_.begin(), Keys_.end(), row.BeginKeys());

    // Construct output values.
    std::copy(MergedValues_.begin(), MergedValues_.end(), row.BeginValues());

    // Construct output timestamps.
    std::copy(MergedTimestamps_.begin(), MergedTimestamps_.end(), row.BeginTimestamps());

    Reset();
    return row;
}

void TVersionedRowMerger::Reset()
{
    PartialValues_.clear();
    DeleteTimestamps_.clear();
    Started_ = false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

