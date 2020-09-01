#pragma once

#include "public.h"

namespace NYT::NTransactionServer {

///////////////////////////////////////////////////////////////////////////////

TError CreateNoSuchTransactionError(TTransactionId transactionId);

[[noreturn]] void ThrowNoSuchTransaction(TTransactionId transactionId);

TError CreatePrerequisiteCheckFailedNoSuchTransactionError(TTransactionId transactionId);

[[noreturn]] void ThrowPrerequisiteCheckFailedNoSuchTransaction(TTransactionId transactionId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
