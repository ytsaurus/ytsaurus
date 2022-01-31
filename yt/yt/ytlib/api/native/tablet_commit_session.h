#pragma once

#include "public.h"

#include "tablet_request_batcher.h"

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/client/chaos_client/public.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

struct TTabletCommitOptions
{
    NTabletClient::TTableReplicaId UpstreamReplicaId;
    NChaosClient::TReplicationCardPtr ReplicationCard;
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

    virtual TFuture<void> Invoke() = 0;
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

} // namespace NYT::NApi::NNative
