#include "stdafx.h"
#include "transaction_listener.h"
#include "transaction.h"

#include <ytlib/misc/error.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

TTransactionListener::TTransactionListener()
    : IsAborted(false)
{ }

void TTransactionListener::ListenTransaction(ITransactionPtr transaction)
{
    YASSERT(transaction);
    transaction->SubscribeAborted(BIND(
    	&TTransactionListener::OnAborted,
    	MakeWeak(this)));
}

void TTransactionListener::OnAborted()
{
    IsAborted = true;
}

void TTransactionListener::CheckAborted() const
{
    if (IsAborted) {
        THROW_ERROR_EXCEPTION("Transaction aborted");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
