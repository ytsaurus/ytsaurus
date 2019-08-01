#pragma once

#include "public.h"

#include <yt/client/api/public.h>

#include <yt/core/concurrency/public.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

//! A simple base class that listens for transaction aborts.
class TTransactionListener
    : public virtual TRefCounted
{
protected:
    //! Starts listening for transaction abort.
    void StartListenTransaction(const NApi::ITransactionPtr& transaction);

    //! Stops listening for transaction abort.
    void StopListenTransaction(const NApi::ITransactionPtr& transaction);

    //! Starts probing transaction periodically.
    void StartProbeTransaction(const NApi::ITransactionPtr& transaction, TDuration probePeriod);

    //! Stops probing transaction.
    void StopProbeTransaction(const NApi::ITransactionPtr& transaction);

    //! Checks if any of transactions that we are listening to were aborted.
    //! If so, raises an exception.
    void ValidateAborted() const;

    //! Returns aborted flag.
    bool IsAborted() const;

    //! Returns the abort error.
    TError GetAbortError() const;

private:
    std::atomic<bool> Aborted_ = {false};

    mutable TSpinLock SpinLock_;
    std::vector<TTransactionId> IgnoredTransactionIds_;
    std::vector<TTransactionId> AbortedTransactionIds_;
    THashMap<TTransactionId, NConcurrency::TPeriodicExecutorPtr> TransactionIdToProbeExecutor_;

    void ProbeTransaction(const NApi::ITransactionPtr& transaction);
    void OnTransactionAborted(TTransactionId transactionId);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
