#include "abort.h"

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

TAbort::TAbort(
    TTransactionId transactionId,
    NRpc::TMutationId mutationId)
    : TransactionId_(transactionId)
    , MutationId_(mutationId)
{ }

TFuture<TSharedRefArray> TAbort::GetAsyncResponseMessage()
{
    return ResponseMessagePromise_;
}

void TAbort::SetResponseMessage(TSharedRefArray message)
{
    ResponseMessagePromise_.Set(std::move(message));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
