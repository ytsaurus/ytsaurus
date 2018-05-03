#include "transaction_listener.h"
#include "private.h"

#include <yt/ytlib/api/transaction.h>

namespace NYT {
namespace NTransactionClient {

using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TransactionClientLogger;

////////////////////////////////////////////////////////////////////////////////

void TTransactionListener::ListenTransaction(const ITransactionPtr& transaction)
{
    LOG_DEBUG("Listening for transaction (TransactionId: %v)",
        transaction->GetId());

    transaction->SubscribeAborted(
        BIND([=, this_ = MakeStrong(this), id = transaction->GetId()] {
            auto guard = Guard(SpinLock_);
            AbortedTransactionIds_.push_back(id);
            Aborted_.store(true);
        }));
}

bool TTransactionListener::IsAborted() const
{
    return Aborted_.load();
}

TError TTransactionListener::GetAbortError() const
{
    auto guard = Guard(SpinLock_);
    if (AbortedTransactionIds_.empty()) {
        return TError();
    } else if (AbortedTransactionIds_.size() == 1) {
        return TError("Transaction %v aborted",
            AbortedTransactionIds_[0]);
    } else {
        return TError("Transactions %v aborted",
            AbortedTransactionIds_);
    }
}

void TTransactionListener::ValidateAborted() const
{
    if (IsAborted()) {
        THROW_ERROR GetAbortError();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
