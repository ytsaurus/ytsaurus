#include "stdafx.h"
#include "transaction_listener.h"
#include "transaction_manager.h"

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

void TTransactionListener::ListenTransaction(TTransactionPtr transaction)
{
    YCHECK(transaction);

    transaction->SubscribeAborted(BIND(
        &TTransactionListener::OnAborted,
        MakeWeak(this)));
}

void TTransactionListener::OnAborted()
{
    IsAborted_ = true;
}

bool TTransactionListener::IsAborted() const
{
    return IsAborted_;
}

void TTransactionListener::ValidateAborted() const
{
    if (IsAborted_) {
        THROW_ERROR_EXCEPTION("Transaction aborted");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
