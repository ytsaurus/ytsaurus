#include "transaction.h"

#include <mapreduce/yt/interface/error_codes.h>

#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/finally_guard.h>
#include <mapreduce/yt/common/wait_proxy.h>

#include <mapreduce/yt/http/requests.h>
#include <mapreduce/yt/http/retry_request.h>

#include <mapreduce/yt/raw_client/raw_requests.h>

#include <util/datetime/base.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TPingRetryPolicy::TPingRetryPolicy(ui32 attemptCount)
    : AttemptCount_(attemptCount)
{ }

void TPingRetryPolicy::NotifyNewAttempt()
{
    ++Attempt_;
}

TMaybe<TDuration> TPingRetryPolicy::GetRetryInterval(const yexception& /*e*/) const
{
    if (AttemptCount_ && Attempt_ >= AttemptCount_) {
        return Nothing();
    }
    return TConfig::Get()->PingTimeout;
}

TMaybe<TDuration> TPingRetryPolicy::GetRetryInterval(const TErrorResponse& e) const
{
    if (AttemptCount_ && Attempt_ >= AttemptCount_) {
        return Nothing();
    }
    if (e.GetError().ContainsErrorCode(NYT::NClusterErrorCodes::NTransactionClient::NoSuchTransaction)) {
        return Nothing();
    }
    if (e.GetError().ContainsErrorCode(NYT::NClusterErrorCodes::Timeout)) {
        return TDuration::MilliSeconds(0);
    }
    return TConfig::Get()->PingTimeout;
}

TString TPingRetryPolicy::GetAttemptDescription() const
{
    TStringStream s;
    s << "attempt " << Attempt_;
    if (AttemptCount_) {
        s << " of " << AttemptCount_;
    }
    return s.Str();
}

////////////////////////////////////////////////////////////////////////////////

TPingableTransaction::TPingableTransaction(
    const TAuth& auth,
    const TTransactionId& parentId,
    const TMaybe<TDuration>& timeout,
    bool pingAncestors,
    bool autoPingable,
    const TMaybe<TString>& title,
    const TMaybe<TNode>& attributes)
    : Auth_(auth)
    , AbortableRegistry_(NDetail::TAbortableRegistry::Get())
{
    TransactionId_ = StartTransaction(
        auth,
        parentId,
        timeout,
        pingAncestors,
        title,
        attributes);

    AbortableRegistry_->Add(
        TransactionId_,
        ::MakeIntrusive<NDetail::TTransactionAbortable>(auth, TransactionId_));

    Running_ = true;

    if (autoPingable) {
        Thread_ = MakeHolder<TThread>(TThread::TParams{Pinger, (void*)this}.SetName("pingable_tx"));
        Thread_->Start();
    }
}

TPingableTransaction::~TPingableTransaction()
{
    try {
        Stop(false);
    } catch (...) {
    }
}

const TTransactionId TPingableTransaction::GetId() const
{
    return TransactionId_;
}

void TPingableTransaction::Commit()
{
    Stop(true);
}

void TPingableTransaction::Abort()
{
    Stop(false);
}

void TPingableTransaction::Stop(bool commit)
{
    if (!Running_) {
        return;
    }

    NDetail::TFinallyGuard g([&] {
        Running_ = false;
        if (Thread_) {
            Thread_->Join();
        }
    });

    if (commit) {
        CommitTransaction(Auth_, TransactionId_);
    } else {
        AbortTransaction(Auth_, TransactionId_);
    }

    AbortableRegistry_->Remove(TransactionId_);
}

void TPingableTransaction::Pinger()
{
    while (Running_) {
        try {
            TPingRetryPolicy retryPolicy;
            PingTx(Auth_, TransactionId_, &retryPolicy);
        } catch (const TErrorResponse& e) {
            // All other errors must be retried by our TPingRetryPolicy.
            Y_VERIFY(e.GetError().ContainsErrorCode(NYT::NClusterErrorCodes::NTransactionClient::NoSuchTransaction));
            break;
        }

        TInstant t = Now();
        while (Running_ && Now() - t < TConfig::Get()->PingInterval) {
            NDetail::TWaitProxy::Sleep(TDuration::MilliSeconds(100));
        }
    }
}

void* TPingableTransaction::Pinger(void* opaque)
{
    static_cast<TPingableTransaction*>(opaque)->Pinger();
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

TYPath Snapshot(const TAuth& auth, const TTransactionId& transactionId, const TYPath& path)
{
    const int maxAttempt = TConfig::Get()->RetryCount;
    for (int attempt = 0; attempt < maxAttempt; ++attempt) {
        bool canRetry = attempt + 1 < maxAttempt;
        try {
            // A race condition is possible if object at path is replaced between
            // calls to Get() and Lock() methods and its id is invalidated
            auto id = NDetail::Get(auth, transactionId, path + "/@id").AsString();
            TYPath result = TString("#") + id;
            try {
                // It is important to lock object-id path (which will be used later)
                // instead of original cypress path (which can be invalidated
                // regardless of snapshot lock)
                NDetail::Lock(auth, transactionId, result, LM_SNAPSHOT);
            } catch (TErrorResponse& e) {
                if (canRetry && e.IsResolveError()) {
                    // Object id got invalidated before Lock(), retry with
                    // updated object id
                    NDetail::TWaitProxy::Sleep(NDetail::GetRetryInterval(e));
                    continue;
                }
                throw;
            }
            return result;
        } catch (TErrorResponse& e) {
            if (canRetry && NDetail::IsRetriable(e)) {
                NDetail::TWaitProxy::Sleep(NDetail::GetRetryInterval(e));
                continue;
            }
            throw;
        }
    }
    Y_FAIL("unreachable");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
