#include "transaction.h"

#include <mapreduce/yt/interface/error_codes.h>

#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/wait_proxy.h>
#include <mapreduce/yt/common/retry_lib.h>

#include <mapreduce/yt/http/requests.h>
#include <mapreduce/yt/http/retry_request.h>

#include <mapreduce/yt/raw_client/raw_requests.h>

#include <util/datetime/base.h>

#include <util/generic/scope.h>

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
    const IClientRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TTransactionId& parentId,
    const TStartTransactionOptions& options)
    : ClientRetryPolicy_(retryPolicy)
    , Auth_(auth)
    , AbortableRegistry_(NDetail::TAbortableRegistry::Get())
    , AbortOnTermination_(true)
{
    auto transactionId = NDetail::NRawClient::StartTransaction(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        auth,
        parentId,
        options);

    auto actualTimeout = options.Timeout_.GetOrElse(TConfig::Get()->TxTimeout);
    Init(auth, transactionId, actualTimeout, options.AutoPingable_);
}

TPingableTransaction::TPingableTransaction(
    const IClientRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TAttachTransactionOptions& options)
    : ClientRetryPolicy_(retryPolicy)
    , Auth_(auth)
    , AbortableRegistry_(NDetail::TAbortableRegistry::Get())
    , AbortOnTermination_(options.AbortOnTermination_)
{
    auto timeoutNode = NDetail::NRawClient::TryGet(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        auth,
        TTransactionId(),
        "#" + GetGuidAsString(transactionId) + "/@timeout",
        TGetOptions());
    if (timeoutNode.IsUndefined()) {
        throw yexception() << "Transaction " << GetGuidAsString(transactionId) << " does not exist";
    }
    auto timeout = TDuration::MilliSeconds(timeoutNode.AsInt64());
    Init(auth, transactionId, timeout, options.AutoPingable_);
}

void TPingableTransaction::Init(
    const TAuth& auth,
    const TTransactionId& transactionId,
    TDuration timeout,
    bool autoPingable)
{
    TransactionId_ = transactionId;

    if (AbortOnTermination_) {
        AbortableRegistry_->Add(
            TransactionId_,
            ::MakeIntrusive<NDetail::TTransactionAbortable>(auth, TransactionId_));
    }

    Running_ = true;

    if (autoPingable) {
        // Compute 'MaxPingInterval_' and 'MinPingInterval_' such that 'pingInterval == (max + min) / 2'.
        auto pingInterval = TConfig::Get()->PingInterval;
        auto safeTimeout = timeout - TDuration::Seconds(5);
        MaxPingInterval_ = Max(pingInterval, Min(safeTimeout, pingInterval * 1.5));
        MinPingInterval_ = pingInterval - (MaxPingInterval_ - pingInterval);

        Thread_ = MakeHolder<TThread>(
            TThread::TParams{Pinger, this}.SetName("pingable_tx"));
        Thread_->Start();
    }
}

TPingableTransaction::~TPingableTransaction()
{
    try {
        Stop(AbortOnTermination_ ? EStopAction::Abort : EStopAction::Detach);
    } catch (...) {
    }
}

const TTransactionId TPingableTransaction::GetId() const
{
    return TransactionId_;
}

void TPingableTransaction::Commit()
{
    Stop(EStopAction::Commit);
}

void TPingableTransaction::Abort()
{
    Stop(EStopAction::Abort);
}

void TPingableTransaction::Detach()
{
    Stop(EStopAction::Detach);
}

void TPingableTransaction::Stop(EStopAction action)
{
    if (!Running_) {
        return;
    }

    Y_DEFER {
        Running_ = false;
        if (Thread_) {
            Thread_->Join();
        }
    };

    switch (action) {
        case EStopAction::Commit:
            NDetail::NRawClient::CommitTransaction(
                ClientRetryPolicy_->CreatePolicyForGenericRequest(),
                Auth_,
                TransactionId_);
            break;
        case EStopAction::Abort:
            NDetail::NRawClient::AbortTransaction(
                ClientRetryPolicy_->CreatePolicyForGenericRequest(),
                Auth_,
                TransactionId_);
            break;
        case EStopAction::Detach:
            // Do nothing.
            break;
    }

    AbortableRegistry_->Remove(TransactionId_);
}

void TPingableTransaction::Pinger()
{
    while (Running_) {
        try {
            auto retryPolicy = MakeIntrusive<TPingRetryPolicy>();
            NDetail::NRawClient::PingTx(retryPolicy, Auth_, TransactionId_);
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

TYPath Snapshot(
    const IClientRetryPolicyPtr& clientRetryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path)
{
    auto lockId = NDetail::NRawClient::Lock(
        clientRetryPolicy->CreatePolicyForGenericRequest(),
        auth,
        transactionId,
        path,
        ELockMode::LM_SNAPSHOT);
    auto lockedNodeId = NDetail::NRawClient::Get(
        clientRetryPolicy->CreatePolicyForGenericRequest(),
        auth,
        transactionId,
        TStringBuilder() << '#' << GetGuidAsString(lockId) << "/@node_id");
    return "#" + lockedNodeId.AsString();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
