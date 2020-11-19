#include "partitioner.h"

#include <yt/ytlib/chunk_client/key_set.h>

#include <yt/client/table_client/key_bound.h>

#include <yt/core/misc/blob_output.h>

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
        for (const auto& key : KeySetReader_.GetKeys()) {
            PartitionUpperBounds_.push_back(
                KeyBoundFromLegacyRow(
                    /* key */key,
                    /* isUpper */true,
                    /* keyLength */Comparator_.GetLength()));
        }
        PartitionUpperBounds_.push_back(TOwningKeyBound::MakeUniversal(/* isUpper */true));

        for (int index = 0; index + 1 < PartitionUpperBounds_.size(); ++index) {
            const auto& partition = PartitionUpperBounds_[index];
            const auto& nextPartition = PartitionUpperBounds_[index + 1];
            // TODO(gritukan): Check if pivots are not equal.
            YT_LOG_FATAL_IF(
                Comparator_.CompareKeyBounds(partition, nextPartition) > 0,
                "Pivot keys order violation (Index: %v, PartitionUpperBound: %v, NextPartitionUpperBound: %v)",
                index,
                partition,
                nextPartition);
        }
    }

    virtual int GetPartitionCount() override
    {
        return PartitionUpperBounds_.size();
    }

    virtual int GetPartitionIndex(TUnversionedRow row) override
    {
        auto key = TKey::FromRow(row, Comparator_.GetLength());

        // Recall upper_bound returns first such iterator it that comp(key, *it).
        auto partitionsIt = std::upper_bound(
            PartitionUpperBounds_.begin(),
            PartitionUpperBounds_.end(),
            key,
            [=] (const TKey& key, const TKeyBound& partitionUpperBound) {
                return Comparator_.TestKey(key, partitionUpperBound);
            }
        );

        YT_VERIFY(partitionsIt != PartitionUpperBounds_.end());
        return partitionsIt - PartitionUpperBounds_.begin();
    }

private:
    const TKeySetReader KeySetReader_;
    std::vector<TOwningKeyBound> PartitionUpperBounds_;
    const TComparator Comparator_;
};

IPartitionerPtr CreateOrderedPartitioner(const TSharedRef& wirePivots, TComparator comparator)
{
    return New<TOrderedPartitioner>(wirePivots, comparator);
}

////////////////////////////////////////////////////////////////////////////////

class THashPartitioner
    : public IPartitioner
{
public:
    THashPartitioner(int partitionCount, int keyColumnCount, int salt)
        : PartitionCount_(partitionCount)
        , KeyColumnCount_(keyColumnCount)
        , Salt_(FarmHash(salt))
    { }

    virtual int GetPartitionCount() override
    {
        return PartitionCount_;
    }

    virtual int GetPartitionIndex(TUnversionedRow row) override
    {
        auto rowHash = GetHash(row, KeyColumnCount_);
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

IPartitionerPtr CreateHashPartitioner(int partitionCount, int keyColumnCount, int salt)
{
    return New<THashPartitioner>(partitionCount, keyColumnCount, salt);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
