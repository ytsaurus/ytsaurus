#include "stdafx.h"
#include "common.h"

namespace NYT {
namespace NTransaction {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger TransactionLogger("Transaction");

TTransactionId NullTransactionId(0, 0, 0, 0);
TTransactionId SysTransactionId(0xffffffff, 0xffffffff, 0xffffffff, 0xffffffff);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransaction
} // namespace NYT

