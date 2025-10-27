#pragma once

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/queue_client/public.h>

#include <yt/yt/core/rpc/channel.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

//! Creates YPath service for queue agent orchid subtree representing queue/consumer #cluster:#objectPath.
//! Object kind can be either "queue" or "consumer".
NYTree::IYPathServicePtr CreateQueueAgentYPathService(
    NRpc::IChannelPtr queueAgentChannel,
    const std::string& cluster,
    const std::string& objectKind,
    const NYPath::TYPath& objectPath);

struct TValidatePushQueueProducerRowsResult
{
    TQueueProducerSequenceNumber LastSequenceNumber = TQueueProducerSequenceNumber(-1);
    i64 SkipRowCount = 0;
};

TValidatePushQueueProducerRowsResult ValidatePushQueueProducerRows(
    const NTableClient::TNameTablePtr& nameTable,
    const TSharedRange<NTableClient::TUnversionedRow>& rows,
    TQueueProducerSequenceNumber lastProducerSequenceNumber,
    std::optional<TQueueProducerSequenceNumber> initialSequenceNumber);

////////////////////////////////////////////////////////////////////////////////

template <class TTable>
TIntrusivePtr<TTable> CreateStateTableClientOrThrow(
    const TWeakPtr<NApi::NNative::IConnection>& connection,
    const std::optional<std::string>& cluster,
    const NYPath::TYPath& path,
    const std::string& user);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
