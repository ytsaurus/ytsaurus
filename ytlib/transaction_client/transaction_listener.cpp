#include "transaction_listener.h"

#include <yt/ytlib/api/transaction.h>

namespace NYT {
namespace NTransactionClient {

using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

void TTransactionListener::ListenTransaction(ITransactionPtr transaction)
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
