#include "stdafx.h"

#include "partitioner.h"

#include <core/misc/blob_output.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TOrderedPartitioner
    : public IPartitioner
{
public:
    TOrderedPartitioner(const std::vector<TOwningKey>* keys, int keyColumnCount)
        : Keys(keys)
        , KeyColumnCount_(keyColumnCount)
    { }

    virtual int GetPartitionCount() override
    {
        return Keys->size() + 1;
    }

    virtual int GetPartitionIndex(const TUnversionedRow& row) override
    {
        auto it = std::upper_bound(
            Keys->begin(),
            Keys->end(),
            row,
            [=] (const TUnversionedRow& row, const TOwningKey& element) {
                return CompareRows(row, element.Get(), KeyColumnCount_) < 0;
            });
        return std::distance(Keys->begin(), it);
    }

private:
    const std::vector<TOwningKey>* Keys;
    const int KeyColumnCount_;

};

std::unique_ptr<IPartitioner> CreateOrderedPartitioner(const std::vector<TOwningKey>* keys, int keyColumnCount)
{
    return std::unique_ptr<IPartitioner>(new TOrderedPartitioner(keys, keyColumnCount));
}

////////////////////////////////////////////////////////////////////////////////

class THashPartitioner
    : public IPartitioner
{
public:
    THashPartitioner(int partitionCount, int keyColumnCount)
        : PartitionCount(partitionCount)
        , KeyColumnCount(keyColumnCount)
    { }

    virtual int GetPartitionCount() override
    {
        return PartitionCount;
    }

    virtual int GetPartitionIndex(const TUnversionedRow& row) override
    {
        return GetHash(row, KeyColumnCount) % PartitionCount;
    }

private:
    int PartitionCount;
    int KeyColumnCount;

};

std::unique_ptr<IPartitioner> CreateHashPartitioner(int partitionCount, int keyColumnCount)
{
    return std::unique_ptr<IPartitioner>(new THashPartitioner(partitionCount, keyColumnCount));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
