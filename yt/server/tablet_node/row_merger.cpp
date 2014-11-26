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

    MergedTimestamps_.resize(SchemaColumnCount_);

    Cleanup();
}

void TUnversionedRowMerger::AddPartialRow(TVersionedRow row)
{
    if (!row)
        return;

    YASSERT(row.GetKeyCount() == KeyColumnCount_);
    YASSERT(row.GetWriteTimestampCount() <= 1);
    YASSERT(row.GetDeleteTimestampCount() <= 1);

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
    if (!Started_) {
        return TUnversionedRow();
    }

    if (LatestWrite_ == NullTimestamp || LatestWrite_ < LatestDelete_) {
        Cleanup();
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

    Cleanup();
    return mergedRow;
}

void TUnversionedRowMerger::Reset()
{
    YCHECK(!Started_);
    Pool_->Clear();
    MergedRow_ = TUnversionedRow();
}

void TUnversionedRowMerger::Cleanup()
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
    Cleanup();
}

void TVersionedRowMerger::AddPartialRow(TVersionedRow row)
{
    if (!row)
        return;

    if (!Started_) {
        Started_ = true;
        YASSERT(row.GetKeyCount() == KeyColumnCount_);
        std::copy(row.BeginKeys(), row.EndKeys(), Keys_.data());
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

    // Sort input values by |(id, timestamp)|.
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
        // Delete timestamps are represented by TheBottom sentinels.
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
                    auto value = MakeVersionedSentinelValue(EValueType::TheBottom, *timestampIt);
                    ColumnValues_.push_back(value);
                    ++timestampIt;
                } 
            }
        }

#ifndef NDEBUG
        // Validate merged list.
        for (auto it = ColumnValues_.begin(); it != ColumnValues_.end(); ++it) {
            YASSERT(it + 1 == ColumnValues_.end() || (it->Timestamp <= (it + 1)->Timestamp));
        }
#endif

        // Compute safety limit by MinDataVersions.
        auto safetyEndIt = ColumnValues_.begin();
        if (ColumnValues_.size() > Config_->MinDataVersions) {
            safetyEndIt = ColumnValues_.end() - Config_->MinDataVersions;
        }

        // Adjust safety limit by MinDataTtl.
        while (safetyEndIt != ColumnValues_.begin()) {
            auto timestamp = (safetyEndIt - 1)->Timestamp;
            if (timestamp < CurrentTimestamp_ &&
                TimestampDiffToDuration(timestamp, CurrentTimestamp_).first > Config_->MinDataTtl)
            {
                break;
            }
            --safetyEndIt;
        }

        // Compute retention limit by MaxDataVersions and MaxDataTtl.
        auto retentionBeginIt = safetyEndIt;
        while (retentionBeginIt != ColumnValues_.begin()) {
            if (std::distance(retentionBeginIt, ColumnValues_.end()) >= Config_->MaxDataVersions)
                break;

            auto timestamp = (retentionBeginIt - 1)->Timestamp;
            if (timestamp < CurrentTimestamp_ &&
                TimestampDiffToDuration(timestamp, CurrentTimestamp_).first > Config_->MaxDataTtl)
                break;

            --retentionBeginIt;
        }

        // Save output values and timestamps.
        for (auto it = retentionBeginIt; it != ColumnValues_.end(); ++it) {
            const auto& value = *it;
            if (value.Type != EValueType::TheBottom) {
                WriteTimestamps_.push_back(value.Timestamp);
                MergedValues_.push_back(*it);
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

    if (MergedValues_.empty() && WriteTimestamps_.empty() && DeleteTimestamps_.empty()) {
        Cleanup();
        return TVersionedRow();
    }

    // Construct output row.
    auto row = TVersionedRow::Allocate(
        Pool_,
        KeyColumnCount_,
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
    YCHECK(!Started_);
    Pool_->Clear();
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

} // namespace NTabletNode
} // namespace NYT

