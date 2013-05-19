#pragma once

#include "public.h"
#include <ytlib/chunk_client/key.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IPartitioner
{
    virtual ~IPartitioner()
    { }

    virtual int GetPartitionCount() = 0;
    virtual int GetPartitionTag(const NChunkClient::TNonOwningKey& key) = 0;
};

std::unique_ptr<IPartitioner> CreateOrderedPartitioner(const std::vector<NChunkClient::TOwningKey>* keys);
std::unique_ptr<IPartitioner> CreateHashPartitioner(int partitionCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
