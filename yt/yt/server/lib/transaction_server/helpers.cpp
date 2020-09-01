#include "helpers.h"

namespace NYT::NTransactionServer {

///////////////////////////////////////////////////////////////////////////////

TError CreateNoSuchTransactionError(TTransactionId transactionId)
{
    return TError(
        NTransactionClient::EErrorCode::NoSuchTransaction,
        "No such transaction %v",
        transactionId);
}

void ThrowNoSuchTransaction(TTransactionId transactionId)
{
    THROW_ERROR(CreateNoSuchTransactionError(transactionId));
}

TError CreatePrerequisiteCheckFailedNoSuchTransactionError(TTransactionId transactionId)
{
    return TError(
        NObjectClient::EErrorCode::PrerequisiteCheckFailed,
        "Prerequisite check failed: transaction %v is missing",
        transactionId);
}

[[noreturn]] void ThrowPrerequisiteCheckFailedNoSuchTransaction(TTransactionId transactionId)
{
    THROW_ERROR(CreatePrerequisiteCheckFailedNoSuchTransactionError(transactionId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
