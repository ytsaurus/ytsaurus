#include "partitioner.h"

#include <yt/core/misc/blob_output.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TOrderedPartitioner
    : public IPartitioner
{
public:
    explicit TOrderedPartitioner(std::vector<TOwningKey> keys)
        : Keys_(std::move(keys))
    { }

    virtual int GetPartitionCount() override
    {
        return Keys_.size() + 1;
    }

    virtual int GetPartitionIndex(TUnversionedRow row) override
    {
        auto it = std::upper_bound(
            Keys_.begin(),
            Keys_.end(),
            row,
            [] (TUnversionedRow row, const TOwningKey& element) {
                return row < element;
            });
        return std::distance(Keys_.begin(), it);
    }

private:
    const std::vector<TOwningKey> Keys_;

};

IPartitionerPtr CreateOrderedPartitioner(std::vector<TOwningKey> keys)
{
    return New<TOrderedPartitioner>(std::move(keys));
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

} // namespace NTableClient
} // namespace NYT
