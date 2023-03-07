#include "partitioner.h"

#include <yt/ytlib/chunk_client/key_set.h>

#include <yt/core/misc/blob_output.h>

namespace NYT::NTableClient {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

class TOrderedPartitioner
    : public IPartitioner
{
public:
    explicit TOrderedPartitioner(const TSharedRef& wirePartitionKeys)
        : KeySetReader_(wirePartitionKeys)
        , Keys_(KeySetReader_.GetKeys())
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
            [] (TUnversionedRow row, const TKey& element) {
                return row < element;
            });
        return std::distance(Keys_.Begin(), it);
    }

private:
    const TKeySetReader KeySetReader_;
    const TRange<TKey> Keys_;
};

IPartitionerPtr CreateOrderedPartitioner(const TSharedRef& wirePartitionKeys)
{
    return New<TOrderedPartitioner>(wirePartitionKeys);
}

////////////////////////////////////////////////////////////////////////////////

class THashPartitioner
    : public IPartitioner
{
public:
    THashPartitioner(int partitionCount, int keyColumnCount)
        : PartitionCount_(partitionCount)
        , KeyColumnCount_(keyColumnCount)
    { }

    virtual int GetPartitionCount() override
    {
        return PartitionCount_;
    }

    virtual int GetPartitionIndex(TUnversionedRow row) override
    {
        return GetHash(row, KeyColumnCount_) % PartitionCount_;
    }

private:
    const int PartitionCount_;
    const int KeyColumnCount_;
};

IPartitionerPtr CreateHashPartitioner(int partitionCount, int keyColumnCount)
{
    return New<THashPartitioner>(partitionCount, keyColumnCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
