#pragma once

#include "public.h"

#include <yt/ytlib/api/public.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

//! A simple base class that listens for transaction aborts.
class TTransactionListener
    : public virtual TRefCounted
{
protected:
    //! Starts listening for transaction abort.
    void ListenTransaction(const NApi::ITransactionPtr& transaction);

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
    std::vector<TTransactionId> AbortedTransactionIds_;

private:
    void OnTransactionAborted(const TTransactionId& id);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
