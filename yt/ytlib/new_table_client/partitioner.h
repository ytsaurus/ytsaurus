#pragma once

#include "public.h"

#include "unversioned_row.h"

#include <core/misc/small_vector.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IPartitioner
{
    virtual ~IPartitioner()
    { }

    virtual int GetPartitionCount() = 0;
    virtual int GetPartitionIndex(const TKey& key) = 0;
};

std::unique_ptr<IPartitioner> CreateOrderedPartitioner(const std::vector<TOwningKey>* keys);
std::unique_ptr<IPartitioner> CreateHashPartitioner(int partitionCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
