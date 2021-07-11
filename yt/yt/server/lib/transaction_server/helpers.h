#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

namespace NYT::NTransactionServer {

///////////////////////////////////////////////////////////////////////////////

TError CreateNoSuchTransactionError(TTransactionId transactionId);

[[noreturn]] void ThrowNoSuchTransaction(TTransactionId transactionId);

TError CreatePrerequisiteCheckFailedNoSuchTransactionError(TTransactionId transactionId);

[[noreturn]] void ThrowPrerequisiteCheckFailedNoSuchTransaction(TTransactionId transactionId);

////////////////////////////////////////////////////////////////////////////////

void LockNodeWithWait(
    const NApi::IClientPtr& client,
    const NApi::ITransactionPtr& transaction,
    const TString& lockPath,
    TDuration checkBackoff,
    TDuration waitTimeout);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
