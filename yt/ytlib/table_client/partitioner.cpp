#include "stdafx.h"
#include "partitioner.h"

#include "key.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

bool operator < (const TOwningKey& partitionKey, const TNonOwningKey& key)
{
    return CompareKeys(partitionKey, key) < 0;
}

class TOrderedPartitioner
    : public IPartitioner
{
public:
    explicit TOrderedPartitioner(const std::vector<TOwningKey>* keys)
        : Keys(keys)
    { }

    virtual int GetPartitionCount() override
    {
        return Keys->size() + 1;
    }

    virtual int GetPartitionTag(const TNonOwningKey& key) override
    {
        auto it = std::upper_bound(Keys->begin(), Keys->end(), key);
        return std::distance(Keys->begin(), it);
    }

private:
    const std::vector<TOwningKey>* Keys;

};

TAutoPtr<IPartitioner> CreateOrderedPartitioner(const std::vector<TOwningKey>* keys)
{
    return new TOrderedPartitioner(keys);
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

    virtual int GetPartitionTag(const TNonOwningKey& key) override
    {
        return key.GetHash() % PartitionCount;
    }

private:
    int PartitionCount;

};

TAutoPtr<IPartitioner> CreateHashPartitioner(int partitionCount)
{
    return new THashPartitioner(partitionCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
