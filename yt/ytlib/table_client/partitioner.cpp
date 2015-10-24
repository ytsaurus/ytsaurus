#include "stdafx.h"

#include "partitioner.h"

#include <core/misc/blob_output.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TOrderedPartitioner
    : public IPartitioner
{
public:
    explicit TOrderedPartitioner(const std::vector<TOwningKey>* keys)
        : Keys_(keys)
    { }

    virtual int GetPartitionCount() override
    {
        return Keys_->size() + 1;
    }

    virtual int GetPartitionIndex(TUnversionedRow row) override
    {
        auto it = std::upper_bound(
            Keys_->begin(),
            Keys_->end(),
            row,
            [] (TUnversionedRow row, const TOwningKey& element) {
                return row < element.Get();
            });
        return std::distance(Keys_->begin(), it);
    }

private:
    const std::vector<TOwningKey>* const Keys_;

};

std::unique_ptr<IPartitioner> CreateOrderedPartitioner(const std::vector<TOwningKey>* keys)
{
    return std::unique_ptr<IPartitioner>(new TOrderedPartitioner(keys));
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

std::unique_ptr<IPartitioner> CreateHashPartitioner(int partitionCount, int keyColumnCount)
{
    return std::unique_ptr<IPartitioner>(new THashPartitioner(partitionCount, keyColumnCount));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
