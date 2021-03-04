#include "public.h"

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

//! Checks if #id represents a valid transaction accepted by masters:
//! the type of #id must be either
//! #EObjectType::Transaction or #EObjectType::NestedTransaction.
bool IsMasterTransactionId(TTransactionId id);

//! Checks if #id represents a valid tablet transaction that can be used
//! for updating dynamic tables (atomic tablet, non-atomic tablet,
//! or top-level master); throws if not.
void ValidateTabletTransactionId(TTransactionId id);

//! Checks if #id represents a valid master transaction (either top-most or nested);
//! throws if not.
void ValidateMasterTransactionId(TTransactionId id);

//! Returns a range of instants containing a given timestamp.
std::pair<TInstant, TInstant> TimestampToInstant(TTimestamp timestamp);

//! Returns a range of timestamps containing a given timestamp.
std::pair<TTimestamp, TTimestamp> InstantToTimestamp(TInstant instant);

//! Returns a range of durations between given timestamps.
std::pair<TDuration, TDuration> TimestampDiffToDuration(TTimestamp loTimestamp, TTimestamp hiTimestamp);

//! Extracts the "unix time" part of the timestamp.
ui64 UnixTimeFromTimestamp(TTimestamp timestamp);

//! Constructs the timestamp from a given unix time (assuming zero counter part).
TTimestamp TimestampFromUnixTime(ui64 time);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
