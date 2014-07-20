#pragma once

#include "public.h"

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
