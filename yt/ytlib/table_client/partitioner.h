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

TAutoPtr<IPartitioner> CreateOrderedPartitioner(const std::vector<NChunkClient::TOwningKey>* keys);
TAutoPtr<IPartitioner> CreateHashPartitioner(int partitionCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
