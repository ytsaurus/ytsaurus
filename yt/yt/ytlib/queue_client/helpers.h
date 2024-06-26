#pragma once

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
    const TString& cluster,
    const TString& objectKind,
    const NYPath::TYPath& objectPath);

struct TValidatePushQueueProducerRowsResult
{
    TQueueProducerSequenceNumber LastSequenceNumber{-1};
    i64 SkipRowCount = 0;
};

TValidatePushQueueProducerRowsResult ValidatePushQueueProducerRows(
    const NTableClient::TNameTablePtr& nameTable,
    const TSharedRange<NTableClient::TUnversionedRow>& rows,
    TQueueProducerSequenceNumber lastProducerSequenceNumber,
    std::optional<TQueueProducerSequenceNumber> initialSequenceNumber);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
