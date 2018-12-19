#pragma once

#include "public.h"

#include <yt/client/api/public.h>

#include <yt/core/rpc/public.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

//! Attaches transaction id to the given request.
/*!
*  #transaction may be null.
*/
void SetTransactionId(NRpc::IClientRequestPtr request, NApi::ITransactionPtr transaction);

//! Returns a range of instants containing a given timestamp.
std::pair<TInstant, TInstant> TimestampToInstant(TTimestamp timestamp);

//! Returns a range of timestamps containing a given timestamp.
std::pair<TTimestamp, TTimestamp> InstantToTimestamp(TInstant instant);

//! Returns a range of durations between given timestamps.
std::pair<TDuration, TDuration> TimestampDiffToDuration(TTimestamp loTimestamp, TTimestamp hiTimestamp);

//! Constructs a tablet transaction id.
TTransactionId MakeTabletTransactionId(
    EAtomicity atomicity,
    NObjectClient::TCellTag cellTag,
    TTimestamp startTimestamp,
    ui32 hash);

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
//! the type of #id must be either
//! #EObjectType::Transaction or #EObjectType::NestedTransaction.
void ValidateMasterTransactionId(TTransactionId id);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
