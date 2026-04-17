#pragma once

#include "public.h"

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

ETransactionType ObjectTypeToTransactionType(NObjectClient::EObjectType objectType);

NObjectClient::EObjectType TransactionTypeToObjectType(
    ETransactionType transactionType,
    bool nested,
    TTransactionId transactionId);

bool ValidateTransactionTypeCoherency(ETransactionType transactionType, TTransactionId transactionId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
