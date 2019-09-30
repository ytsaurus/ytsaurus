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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
