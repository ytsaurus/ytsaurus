#pragma once

#include "public.h"
#include "transaction_manager.h"

#include <core/rpc/public.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

//! Attaches transaction id to the given request.
/*!
*  #transaction may be null.
*/
void SetTransactionId(NRpc::IClientRequestPtr request, TTransactionPtr transaction);

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
TTimestamp TimestampFromTransactionId(const TTransactionId& id);

//! Computes atomicity level for a given transaction.
EAtomicity AtomicityFromTransactionId(const TTransactionId& id);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
