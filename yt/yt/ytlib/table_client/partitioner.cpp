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
    TOrderedPartitioner(const TSharedRef& wirePivots, TComparator comparator)
        : KeySetReader_(wirePivots)
        , Comparator_(comparator)
    {
        PartitionLowerBounds_.push_back(TOwningKeyBound::MakeUniversal(/*isUpper*/false));

        for (const auto& key : KeySetReader_->GetKeys()) {
            PartitionLowerBounds_.push_back(
                KeyBoundFromLegacyRow(
                    /*key*/ key,
                    /*isUpper*/ false,
                    /*keyLength*/ Comparator_.GetLength()));
        }

        ValidateKeyBounds();
    }

    TOrderedPartitioner(std::vector<TOwningKeyBound> partitionLowerBounds, TComparator comparator)
        : PartitionLowerBounds_(std::move(partitionLowerBounds))
        , Comparator_(comparator)
    {
        ValidateKeyBounds();
    }

    int GetPartitionCount() override
    {
        return PartitionLowerBounds_.size();
    }

    int GetPartitionIndex(TUnversionedRow row) override
    {
        auto key = TKey::FromRow(row, Comparator_.GetLength());

        // Find first partition that our key does not belong to.
        // Recall upper_bound returns first such iterator it that comp(key, *it).
        auto partitionsIt = std::upper_bound(
            PartitionLowerBounds_.begin(),
            PartitionLowerBounds_.end(),
            key,
            [this] (const TKey& key, const TKeyBound& partitionLowerBound) {
                return !Comparator_.TestKey(key, partitionLowerBound);
            }
        );

        YT_VERIFY(partitionsIt != PartitionLowerBounds_.begin());
        return partitionsIt - PartitionLowerBounds_.begin() - 1;
    }

private:
    const std::optional<TKeySetReader> KeySetReader_;
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

IPartitionerPtr CreateOrderedPartitioner(const TSharedRef& wirePivots, TComparator comparator)
{
    return New<TOrderedPartitioner>(wirePivots, comparator);
}

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

    int GetPartitionCount() override
    {
        return PartitionCount_;
    }

    int GetPartitionIndex(TUnversionedRow row) override
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

} // namespace NYT::NTableClient
