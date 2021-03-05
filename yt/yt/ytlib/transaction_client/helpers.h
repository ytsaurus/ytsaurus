#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

//! Attaches transaction id to the given request.
/*!
*  #transaction may be null.
*/
void SetTransactionId(NRpc::IClientRequestPtr request, NApi::ITransactionPtr transaction);

void SetPrerequisites(const NRpc::IClientRequestPtr& request, const NApi::TPrerequisiteOptions& options);

//! Constructs a tablet transaction id.
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

//! Checks if #id represents a valid transaction accepted by tablets:
//! the type of #id must be either
//! #EObjectType::Transaction, #EObjectType::AtomicTabletTransaction,
//! or #EObjectType::NonAtomicTabletTransaction.
void ValidateTabletTransactionId(TTransactionId id);

//! Checks if #id represents a valid transaction accepted by masters:
//! the type of #id must be one of
//! #EObjectType::Transaction, #EObjectType::NestedTransaction,
//! #EObjectType::UploadTransaction, or #EObjectType::UploadNestedTransaction.
void ValidateMasterTransactionId(TTransactionId id);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
