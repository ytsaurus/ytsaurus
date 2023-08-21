#pragma once

#include "public.h"

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

void SetTransactionId(
    NRpc::IClientRequestPtr request,
    NApi::ITransactionPtr transaction);

void SetSuppressUpstreamSyncs(
    const NObjectClient::TObjectServiceProxy::TReqExecuteBatchBasePtr& request,
    const NApi::TTransactionalOptions& options);

void SetPrerequisites(
    const NRpc::IClientRequestPtr& request,
    const NApi::TPrerequisiteOptions& options);

TTransactionId MakeTabletTransactionId(
    EAtomicity atomicity,
    NObjectClient::TCellTag cellTag,
    TTimestamp startTimestamp,
    ui32 hash);

//! Constructs the id of the transaction externalized by cell #externalizingCellTag.
TTransactionId MakeExternalizedTransactionId(
    TTransactionId originalId,
    NObjectClient::TCellTag externalizingCellTag);

//! Undones the effect of #MakeExternalizedTransactionId.
TTransactionId OriginalFromExternalizedTransactionId(TTransactionId externalizedId);

//! Extracts the (start) timestamp from transaction id.
//! #id represent a well-formed tablet transaction id.
TTimestamp TimestampFromTransactionId(TTransactionId id);

//! Computes atomicity level for a given transaction.
EAtomicity AtomicityFromTransactionId(TTransactionId id);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
