#include "partitioner.h"

#include <yt/ytlib/chunk_client/key_set.h>

#include <yt/core/misc/blob_output.h>

namespace NYT::NTableClient {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

// IMPORTANT: Think twice before changing logic in any of the classes below!
// You may occasionally end up in a situation when partition jobs in same operation
// act differently (for example, during node rolling update) leading to partitioning
// invariant violation.

class TOrderedPartitioner
    : public IPartitioner
{
public:
    TOrderedPartitioner(const TSharedRef& wirePartitionKeys, int keyColumnCount)
        : KeySetReader_(wirePartitionKeys)
        , Keys_(KeySetReader_.GetKeys())
        , KeyColumnCount_(keyColumnCount)
    { }

    virtual int GetPartitionCount() override
    {
        return Keys_.Size() + 1;
    }

    virtual int GetPartitionIndex(TUnversionedRow row) override
    {
        auto it = std::upper_bound(
            Keys_.Begin(),
            Keys_.End(),
            row,
            [=] (TUnversionedRow row, const TKey& element) {
                // We consider only key prefix of the row; note that remaining
                // values of the row may be incomparable at all (like double NaN).
                return CompareRows(
                    row.Begin(),
                    row.Begin() + KeyColumnCount_,
                    element.Begin(),
                    element.End()) < 0;
            });
        return std::distance(Keys_.Begin(), it);
    }

private:
    const TKeySetReader KeySetReader_;
    const TRange<TKey> Keys_;
    const int KeyColumnCount_;
};

IPartitionerPtr CreateOrderedPartitioner(const TSharedRef& wirePartitionKeys, int keyColumnCount)
{
    return New<TOrderedPartitioner>(wirePartitionKeys, keyColumnCount);
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
        // TODO(gritukan): Seems like many map-reduce tests rely on distribution keys by partitions,
        // so I'll keep the old hash function for root partition task for a while.
        // NB: This relies on the fact that FarmHash(0) = 0.
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
