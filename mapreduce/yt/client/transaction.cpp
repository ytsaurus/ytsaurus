#include "transaction.h"

#include <mapreduce/yt/interface/error_codes.h>

#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/finally_guard.h>
#include <mapreduce/yt/common/wait_proxy.h>
#include <mapreduce/yt/common/retry_lib.h>

#include <mapreduce/yt/http/requests.h>
#include <mapreduce/yt/http/retry_request.h>

#include <mapreduce/yt/raw_client/raw_requests.h>

#include <util/datetime/base.h>

#include <util/random/random.h>

#include <util/string/builder.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TPingRetryPolicy::TPingRetryPolicy(ui32 attemptCount)
    : AttemptCount_(attemptCount)
{ }

void TPingRetryPolicy::NotifyNewAttempt()
{
    ++Attempt_;
}

TMaybe<TDuration> TPingRetryPolicy::OnGenericError(const yexception& /*e*/)
{
    if (AttemptCount_ && Attempt_ >= AttemptCount_) {
        return Nothing();
    }
    return TConfig::Get()->PingTimeout;
}

void TPingRetryPolicy::OnIgnoredError(const NYT::TErrorResponse& /*e*/)
{
    --Attempt_;
}

TMaybe<TDuration> TPingRetryPolicy::OnRetriableError(const TErrorResponse& e)
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

    {
        // Compute 'MaxPingInterval_' and 'MinPingInterval_' such that 'pingInterval == (max + min) / 2'.
        auto pingInterval = TConfig::Get()->PingInterval;
        auto actualTimeout = timeout.GetOrElse(TConfig::Get()->TxTimeout);
        auto safeTimeout = actualTimeout - TDuration::Seconds(5);
        MaxPingInterval_ = Max(pingInterval, Min(safeTimeout, pingInterval * 1.5));
        MinPingInterval_ = pingInterval - (MaxPingInterval_ - pingInterval);
    }

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
            auto retryPolicy = MakeIntrusive<TPingRetryPolicy>();
            NDetail::NRawClient::PingTx(Auth_, TransactionId_, retryPolicy);
        } catch (const TErrorResponse& e) {
            // All other errors must be retried by our TPingRetryPolicy.
            Y_VERIFY(
                !IsRetriable(e) ||
                e.GetError().ContainsErrorCode(NYT::NClusterErrorCodes::NTransactionClient::NoSuchTransaction),
                "Unexpected exception: %s",
                e.what());
            break;
        }

        TDuration pingInterval = MinPingInterval_ + (MaxPingInterval_ - MinPingInterval_) * RandomNumber<float>();
        TInstant t = Now();
        while (Running_ && Now() - t < pingInterval) {
            NDetail::TWaitProxy::Get()->Sleep(TDuration::MilliSeconds(100));
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
    auto lockId = NDetail::NRawClient::Lock(auth, transactionId, path, ELockMode::LM_SNAPSHOT);
    auto lockedNodeId = NDetail::NRawClient::Get(
        auth,
        transactionId,
        TStringBuilder() << '#' << GetGuidAsString(lockId) << "/@node_id");
    return "#" + lockedNodeId.AsString();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
