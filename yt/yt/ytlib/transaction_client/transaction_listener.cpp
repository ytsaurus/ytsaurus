#include "transaction_listener.h"
#include "private.h"

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NTransactionClient {

using namespace NApi;
using namespace NConcurrency;

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

void TTransactionListener::StartProbeTransaction(const ITransactionPtr& transaction, TDuration probePeriod)
{
    YT_LOG_DEBUG("Started probing transaction (TransactionId: %v, ProbePeriod: %v)",
        transaction->GetId(),
        probePeriod);

    auto guard = Guard(SpinLock_);
    if (TransactionIdToProbeExecutor_.contains(transaction->GetId())) {
        return;
    }
    auto executor = New<TPeriodicExecutor>(
        GetSyncInvoker(),
        BIND(&TTransactionListener::ProbeTransaction, MakeWeak(this), transaction),
        probePeriod);
    YT_VERIFY(TransactionIdToProbeExecutor_.emplace(transaction->GetId(), executor).second);
    guard.Release();
    executor->Start();
}

void TTransactionListener::StopProbeTransaction(const ITransactionPtr& transaction)
{
    YT_LOG_DEBUG("Stopped probing transaction (TransactionId: %v)",
        transaction->GetId());

    auto guard = Guard(SpinLock_);
    auto it = TransactionIdToProbeExecutor_.find(transaction->GetId());
    if (it == TransactionIdToProbeExecutor_.end()) {
        return;
    }
    auto executor = it->second;
    TransactionIdToProbeExecutor_.erase(it);
    IgnoredTransactionIds_.push_back(transaction->GetId());
    guard.Release();
    YT_UNUSED_FUTURE(executor->Stop());
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

void TTransactionListener::ProbeTransaction(const ITransactionPtr& transaction)
{
    // TODO(babenko): replace with Probe
    transaction->Ping()
        .Subscribe(BIND([this, this_ = MakeStrong(this), transactionId = transaction->GetId()] (const TError& error) {
            if (error.GetCode() == NTransactionClient::EErrorCode::NoSuchTransaction) {
                OnTransactionAborted(transactionId, error);
            }
        }));
}

void TTransactionListener::OnTransactionAborted(TTransactionId transactionId, const TError& error)
{
    auto guard = Guard(SpinLock_);
    if (std::find(IgnoredTransactionIds_.begin(), IgnoredTransactionIds_.end(), transactionId) != IgnoredTransactionIds_.end()) {
        return;
    }
    YT_LOG_DEBUG(error, "Transaction abort detected (TransactionId: %v)",
        transactionId);
    AbortedTransactionIds_.push_back(transactionId);
    Aborted_.store(true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
