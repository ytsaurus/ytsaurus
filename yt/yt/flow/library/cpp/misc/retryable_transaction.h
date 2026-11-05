#pragma once

#include "public.h"

#include <yt/yt/client/api/transaction.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TCommitAttemptResult
{
    NApi::ITransactionPtr Transaction;
    TErrorOr<NApi::TTransactionCommitResult> CommitResult;
};

////////////////////////////////////////////////////////////////////////////////

class IRetryableTransaction
    : public virtual TRefCounted
    , public NApi::IDynamicTableTransaction
{
public:
    //! Callback invoked synchronously from the commit thread right after a commit attempt.
    //! Must be cheap and non-blocking; any waits or heavy work will stall the commit loop
    //! and may break retry timing.
    using TOnAttemptResultCallback = TCallback<void(const TCommitAttemptResult& result)>;

    // |transactionWriter| callback is called at each attempt of committing.
    // So it can read tables in transaction and perform CAS-like logic.
    virtual void Apply(TCallback<void(const NApi::ITransactionPtr&)> transactionWriter) = 0;

    // Write saved data to real |transaction|.
    // Can be called repeatedly. It writes the same date every time.
    // Must be called after all modifying calls.
    virtual void DoAttempt(const NApi::ITransactionPtr& transaction) = 0;

    // Chech if there is no saved data in transaction.
    virtual bool IsEmpty() = 0;

    // Register a callback fired after every commit attempt.
    virtual void SubscribeOnAttemptResult(TOnAttemptResultCallback callback) = 0;

    //! The code that applies this transaction is responsible for calling this method
    //! after every commit attempt.
    virtual void OnAttemptResult(const TCommitAttemptResult& result) = 0;
};

DEFINE_REFCOUNTED_TYPE(IRetryableTransaction)

////////////////////////////////////////////////////////////////////////////////

IRetryableTransactionPtr CreateRetryableTransaction();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
