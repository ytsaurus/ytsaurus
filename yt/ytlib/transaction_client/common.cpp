#include "stdafx.h"
#include "common.h"

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger TransactionClientLogger("TransactionClient");

TTransactionId NullTransactionId(0, 0, 0, 0);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT

