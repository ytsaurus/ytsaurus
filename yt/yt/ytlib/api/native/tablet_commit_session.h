#pragma once

#include "public.h"

#include "client.h"
#include "tablet_request_batcher.h"

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/client/chaos_client/public.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

struct TTabletCommitOptions
{
    NTabletClient::TTableReplicaId UpstreamReplicaId;
    NChaosClient::TReplicationCardPtr ReplicationCard;

    std::vector<NTransactionClient::TTransactionId> PrerequisiteTransactionIds;
};

////////////////////////////////////////////////////////////////////////////////

struct ITabletCommitSession
    : public TRefCounted
{
    virtual void SubmitUnversionedRow(
        NTableClient::EWireProtocolCommand command,
        NTableClient::TUnversionedRow row,
        NTableClient::TLockMask lockMask) = 0;

    virtual void SubmitVersionedRow(
        NTableClient::TTypeErasedRow row) = 0;

    virtual void PrepareRequests() = 0;

    virtual void MemorizeHunkInfo(const NTableClient::THunkChunksInfo& hunkInfo) = 0;

    virtual TFuture<void> Invoke(int retryIndex = 0) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITabletCommitSession)

////////////////////////////////////////////////////////////////////////////////

ITabletCommitSessionPtr CreateTabletCommitSession(
    IClientPtr client,
    TTabletCommitOptions options,
    TWeakPtr<NTransactionClient::TTransaction> transaction,
    ICellCommitSessionProviderPtr cellCommitSessionProvider,
    NTabletClient::TTabletInfoPtr tabletInfo,
    NTabletClient::TTableMountInfoPtr tableInfo,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

TFuture<void> CommitTabletSessions(
    std::vector<ITabletCommitSessionPtr> sessions,
    TExponentialBackoffOptions backoffOptions,
    NLogging::TLogger logger,
    TTransactionCounters counters);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
