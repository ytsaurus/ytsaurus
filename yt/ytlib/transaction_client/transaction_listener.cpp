#include "transaction_listener.h"
#include "private.h"

#include <yt/client/api/transaction.h>

namespace NYT::NTransactionClient {

using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TransactionClientLogger;

////////////////////////////////////////////////////////////////////////////////

void TTransactionListener::StartListenTransaction(const ITransactionPtr& transaction)
{
    YT_LOG_DEBUG("Started listening for transaction (TransactionId: %v)",
        transaction->GetId());

    transaction->SubscribeAborted(
        BIND(&TTransactionListener::OnTransactionAborted, MakeWeak(this), transaction->GetId()));
}

void TTransactionListener::StopListenTransaction(const ITransactionPtr& transaction)
{
    YT_LOG_DEBUG("Stopped listening for transaction (TransactionId: %v)",
        transaction->GetId());

    auto guard = Guard(SpinLock_);
    IgnoredTransactionIds_.push_back(transaction->GetId());
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

void TTransactionListener::OnTransactionAborted(TTransactionId id)
{
    auto guard = Guard(SpinLock_);
    if (std::find(IgnoredTransactionIds_.begin(), IgnoredTransactionIds_.end(), id) != IgnoredTransactionIds_.end()) {
        return;
    }
    AbortedTransactionIds_.push_back(id);
    Aborted_.store(true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
