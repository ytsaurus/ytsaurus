#include "partitioner.h"

#include "key_set.h"

#include <yt/yt/client/table_client/key_bound.h>

#include <yt/yt/core/misc/blob_output.h>

namespace NYT::NTableClient {

using namespace NChunkClient;

// IMPORTANT: Think twice before changing logic in any of the classes below!
// You may occasionally end up in a situation when partition jobs in same operation
// act differently (for example, during node rolling update) leading to partitioning
// invariant violation.

////////////////////////////////////////////////////////////////////////////////

// Used only for YT_LOG_FATAL below.
const static NLogging::TLogger Logger("Partitioner");

////////////////////////////////////////////////////////////////////////////////

class TOrderedPartitioner
    : public IPartitioner
{
public:
    TOrderedPartitioner(std::vector<TOwningKeyBound> partitionLowerBounds, TComparator comparator)
        : PartitionLowerBounds_(std::move(partitionLowerBounds))
        , Comparator_(std::move(comparator))
    {
        ValidateKeyBounds();
    }

    int GetPartitionCount() const override
    {
        return PartitionLowerBounds_.size();
    }

    int GetPartitionIndex(TUnversionedRow row) const override
    {
        auto key = TKey::FromRow(row, Comparator_.GetLength());

        // Find first partition that our key does not belong to.
        // Recall upper_bound returns first such iterator it that comp(key, *it).
        const auto* partitionsIt = std::upper_bound(
            PartitionLowerBounds_.begin(),
            PartitionLowerBounds_.end(),
            key,
            [this] (const TKey& key, const TKeyBound& partitionLowerBound) {
                return !Comparator_.TestKey(key, partitionLowerBound);
            });

        YT_VERIFY(partitionsIt != PartitionLowerBounds_.begin());
        return partitionsIt - PartitionLowerBounds_.begin() - 1;
    }

private:
    std::vector<TOwningKeyBound> PartitionLowerBounds_;
    const TComparator Comparator_;

    void ValidateKeyBounds()
    {
        for (int index = 0; index + 1 < std::ssize(PartitionLowerBounds_); ++index) {
            const auto& partition = PartitionLowerBounds_[index];
            const auto& nextPartition = PartitionLowerBounds_[index + 1];
            // TODO(gritukan): Check if pivots are not equal.
            YT_LOG_FATAL_IF(
                Comparator_.CompareKeyBounds(partition, nextPartition) > 0,
                "Pivot keys order violation (Index: %v, PartitionLowerBound: %v, NextPartitionLowerBound: %v)",
                index,
                partition,
                nextPartition);
        }
    }
};

IPartitionerPtr CreateOrderedPartitioner(std::vector<TOwningKeyBound> partitionLowerBounds, TComparator comparator)
{
    return New<TOrderedPartitioner>(std::move(partitionLowerBounds), comparator);
}

////////////////////////////////////////////////////////////////////////////////

class THashPartitioner
    : public IPartitioner
{
public:
    THashPartitioner(int partitionCount, int keyColumnCount, TFingerprint salt)
        : PartitionCount_(partitionCount)
        , KeyColumnCount_(keyColumnCount)
        , Salt_(FarmHash(salt))
    { }

    int GetPartitionCount() const override
    {
        return PartitionCount_;
    }

    int GetPartitionIndex(TUnversionedRow row) const override
    {
        int count = std::min(KeyColumnCount_, static_cast<int>(row.GetCount()));
        auto rowHash = GetFarmFingerprint(row.FirstNElements(count));
        if (Salt_ != 0) {
            rowHash = FarmHash(rowHash ^ Salt_);
        }
        return rowHash % PartitionCount_;
    }

private:
    const int PartitionCount_;
    const int KeyColumnCount_;
    const TFingerprint Salt_;
};

IPartitionerPtr CreateHashPartitioner(int partitionCount, int keyColumnCount, TFingerprint salt)
{
    return New<THashPartitioner>(partitionCount, keyColumnCount, salt);
}

////////////////////////////////////////////////////////////////////////////////

class TColumnBasedPartitioner
    : public IPartitioner
{
public:
    TColumnBasedPartitioner(int partitionCount, int partitionColumnId)
        : PartitionCount_(partitionCount)
        , PartitionColumnId_(partitionColumnId)
    { }

    int GetPartitionCount() const override
    {
        return PartitionCount_;
    }

    int GetPartitionIndex(TUnversionedRow row) const override
    {
        for (auto value : row) {
            if (value.Id == PartitionColumnId_) {
                if (value.Type != EValueType::Uint64 &&
                    value.Type != EValueType::Int64) [[unlikely]]
                {
                    THROW_ERROR_EXCEPTION(
                        "Invalid partition column value type: expected type \"int64\" or \"uint64\", actual type %Qlv",
                        value.Type);
                }

                if (value.Type == EValueType::Int64 &&
                    value.Data.Int64 < 0) [[unlikely]]
                {
                    THROW_ERROR_EXCEPTION(
                        "Received negative partition index %v",
                        value.Data.Int64);
                }

                if (value.Data.Uint64 >= static_cast<ui64>(PartitionCount_)) [[unlikely]] {
                    THROW_ERROR_EXCEPTION(
                        "Partition index is out of bounds: %v >= %v",
                        value.Data.Uint64,
                        PartitionCount_);
                }

                return value.Data.Uint64;
            }
        }

        THROW_ERROR_EXCEPTION("Row does not contain partition column");
    }

private:
    const int PartitionCount_;
    const int PartitionColumnId_;
};

IPartitionerPtr CreateColumnBasedPartitioner(int partitionCount, int partitionColumnId)
{
    return New<TColumnBasedPartitioner>(partitionCount, partitionColumnId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
