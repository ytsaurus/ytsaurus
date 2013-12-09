#pragma once

#include "public.h"

#include <ytlib/new_table_client/row.h>

#include <core/misc/small_vector.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

typedef TSmallVector<NVersionedTableClient::TUnversionedValue, 5> TPartitionKey;

struct IPartitioner
{
    virtual ~IPartitioner()
    { }

    virtual int GetPartitionCount() = 0;
    virtual int GetPartitionTag(const TPartitionKey& key) = 0;
};

std::unique_ptr<IPartitioner> CreateOrderedPartitioner(const std::vector<NVersionedTableClient::TOwningKey>* keys);
std::unique_ptr<IPartitioner> CreateHashPartitioner(int partitionCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
