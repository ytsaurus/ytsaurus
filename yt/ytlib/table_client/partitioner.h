#pragma once

#include "public.h"
#include "unversioned_row.h"

#include <yt/core/misc/small_vector.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IPartitioner
    : public virtual TRefCounted
{
    virtual int GetPartitionCount() = 0;
    virtual int GetPartitionIndex(TUnversionedRow row) = 0;
};

DEFINE_REFCOUNTED_TYPE(IPartitioner)

IPartitionerPtr CreateOrderedPartitioner(std::vector<TOwningKey> keys);
IPartitionerPtr CreateOrderedPartitioner(const TSharedRef& wirePartitionKeys);

IPartitionerPtr CreateHashPartitioner(int partitionCount, int keyColumnCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
