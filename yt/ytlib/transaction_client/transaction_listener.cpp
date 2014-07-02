#include "stdafx.h"
#include "transaction_listener.h"
#include "transaction_manager.h"

#include <core/misc/error.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

TTransactionListener::TTransactionListener()
    : IsAborted_(false)
{ }

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

void TTransactionListener::CheckAborted() const
{
    if (IsAborted_) {
        THROW_ERROR_EXCEPTION("Transaction aborted");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
