#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

TError CreateNoSuchTransactionError(TTransactionId transactionId);

[[noreturn]] void ThrowNoSuchTransaction(TTransactionId transactionId);

[[noreturn]] void ThrowPrerequisiteCheckFailedNoSuchTransaction(TTransactionId transactionId);

[[noreturn]] void ThrowTransactionIsDoomed(TTransactionId transactionId, bool isPrerequisite = false);

////////////////////////////////////////////////////////////////////////////////

void LockNodeWithWait(
    const NApi::IClientPtr& client,
    const NApi::ITransactionPtr& transaction,
    const TString& lockPath,
    TDuration checkBackoff,
    TDuration waitTimeout);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
