#pragma once

#include "public.h"
#include "key.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IPartitioner
{
    ~IPartitioner()
    { }

    virtual int GetPartitionCount() = 0;
    virtual int GetPartitionTag(const TNonOwningKey& key) = 0;
};

TAutoPtr<IPartitioner> CreateOrderedPartitioner(const std::vector<TOwningKey>* keys);
TAutoPtr<IPartitioner> CreateHashPartitioner(int partitionCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
