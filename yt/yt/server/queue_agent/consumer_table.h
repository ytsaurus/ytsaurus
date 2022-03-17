#pragma once

#include "private.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

struct TPartitionInfo
{
    i64 PartitionIndex = -1;
    i64 NextRowIndex = -1;
    //! Latest time instant the corresponding partition was consumed.
    TInstant LastConsumeTime;
};

////////////////////////////////////////////////////////////////////////////////

//! Interface representing a consumer table.
struct IConsumerTable
    : public TRefCounted
{
    //! Collect partition infos. If there are entries in the consumer table with partition
    //! indices outside of range [0, expectedPartitionCount), they will be ignored.
    virtual TFuture<std::vector<TPartitionInfo>> CollectPartitions(int expectedPartitionCount, bool withLastConsumeTime = false) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IConsumerTable);

////////////////////////////////////////////////////////////////////////////////

IConsumerTablePtr CreateConsumerTable(
    const NApi::IClientPtr& client,
    const NYPath::TYPath& path,
    const NTableClient::TTableSchema& schema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
