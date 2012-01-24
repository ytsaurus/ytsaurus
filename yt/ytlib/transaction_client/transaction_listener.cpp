#include "stdafx.h"
#include "transaction_listener.h"

#include <ytlib/actions/action_util.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

TTransactionListener::TTransactionListener()
    : IsAborted(false)
{ }

void TTransactionListener::ListenTransaction(ITransaction* transaction)
{
    YASSERT(transaction);
    transaction->SubscribeAborted(FromMethod(
        &TTransactionListener::OnAborted,
        // TODO(babenko): use AsWeak
        TWeakPtr<TTransactionListener>(this)));
}

void TTransactionListener::OnAborted()
{
    IsAborted = true;
}

void TTransactionListener::CheckAborted() const
{
    if (IsAborted) {
        ythrow yexception() << "Transaction aborted";
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
