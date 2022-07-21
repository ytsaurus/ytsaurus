#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NQueueClient {

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
struct IConsumerClient
    : public TRefCounted
{
    //! Advance the offset for the given partition, setting it to a new value within the given transaction.
    //!
    //! If oldOffset is specified, the current offset is read (within the transaction) and compared with oldOffset.
    //! If they are equal, the new offset is written, otherwise an exception is thrown.
    virtual void Advance(
        const NApi::ITransactionPtr& transaction,
        int partitionIndex,
        std::optional<i64> oldOffset,
        i64 newOffset) const = 0;

    //! Collect partition infos. If there are entries in the consumer table with partition
    //! indices outside of range [0, expectedPartitionCount), they will be ignored.
    virtual TFuture<std::vector<TPartitionInfo>> CollectPartitions(
        const NApi::IClientPtr& client,
        int expectedPartitionCount,
        bool withLastConsumeTime = false) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IConsumerClient);

////////////////////////////////////////////////////////////////////////////////

IConsumerClientPtr CreateConsumerClient(
    const NYPath::TYPath& path,
    const NTableClient::TTableSchema& schema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
