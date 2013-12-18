#pragma once

#include "public.h"

#include <ytlib/new_table_client/unversioned_row.h>

#include <core/misc/small_vector.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IPartitioner
{
    virtual ~IPartitioner()
    { }

    virtual int GetPartitionCount() = 0;
    virtual int GetPartitionTag(const NVersionedTableClient::TKey& key) = 0;
};

std::unique_ptr<IPartitioner> CreateOrderedPartitioner(const std::vector<NVersionedTableClient::TOwningKey>* keys);
std::unique_ptr<IPartitioner> CreateHashPartitioner(int partitionCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
