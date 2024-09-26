#pragma once

#include "public.h"

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IPartitioner
    : public virtual TRefCounted
{
    virtual int GetPartitionCount() = 0;
    virtual int GetPartitionIndex(TUnversionedRow row) = 0;
};

DEFINE_REFCOUNTED_TYPE(IPartitioner)

////////////////////////////////////////////////////////////////////////////////

IPartitionerPtr CreateOrderedPartitioner(std::vector<TOwningKeyBound> partitionLowerBound, TComparator comparator);

////////////////////////////////////////////////////////////////////////////////

IPartitionerPtr CreateHashPartitioner(int partitionCount, int keyColumnCount, TFingerprint salt);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
