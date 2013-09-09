#include "stdafx.h"
#include "partitioner.h"

#include <core/misc/blob_output.h>

namespace NYT {

namespace NChunkClient {
////////////////////////////////////////////////////////////////////////////////

bool operator < (const TOwningKey& partitionKey, const TNonOwningKey& key)
{
    return CompareKeys(partitionKey, key) < 0;
}

////////////////////////////////////////////////////////////////////////////////
}


namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TOrderedPartitioner
    : public IPartitioner
{
public:
    explicit TOrderedPartitioner(const std::vector<NChunkClient::TOwningKey>* keys)
        : Keys(keys)
    { }

    virtual int GetPartitionCount() override
    {
        return Keys->size() + 1;
    }

    virtual int GetPartitionTag(const NChunkClient::TNonOwningKey& key) override
    {
        auto it = std::upper_bound(Keys->begin(), Keys->end(), key);
        return std::distance(Keys->begin(), it);
    }

private:
    const std::vector<NChunkClient::TOwningKey>* Keys;

};

std::unique_ptr<IPartitioner> CreateOrderedPartitioner(const std::vector<NChunkClient::TOwningKey>* keys)
{
    return std::unique_ptr<IPartitioner>(new TOrderedPartitioner(keys));
}

////////////////////////////////////////////////////////////////////////////////

class THashPartitioner
    : public IPartitioner
{
public:
    explicit THashPartitioner(int partitionCount)
        : PartitionCount(partitionCount)
    { }

    virtual int GetPartitionCount() override
    {
        return PartitionCount;
    }

    virtual int GetPartitionTag(const NChunkClient::TNonOwningKey& key) override
    {
        return key.GetHash() % PartitionCount;
    }

private:
    int PartitionCount;

};

std::unique_ptr<IPartitioner> CreateHashPartitioner(int partitionCount)
{
    return std::unique_ptr<IPartitioner>(new THashPartitioner(partitionCount));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
