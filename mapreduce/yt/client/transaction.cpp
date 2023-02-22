#include "transaction.h"

#include "transaction_pinger.h"

#include <mapreduce/yt/interface/config.h>
#include <mapreduce/yt/interface/error_codes.h>

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

TPingableTransaction::TPingableTransaction(
    const IClientRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TTransactionId& parentId,
    ITransactionPingerPtr transactionPinger,
    const TStartTransactionOptions& options)
    : ClientRetryPolicy_(retryPolicy)
    , Auth_(auth)
    , AbortableRegistry_(NDetail::TAbortableRegistry::Get())
    , AbortOnTermination_(true)
    , AutoPingable_(options.AutoPingable_)
    , Pinger_(std::move(transactionPinger))
{
    auto transactionId = NDetail::NRawClient::StartTransaction(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        auth,
        parentId,
        options);

    auto actualTimeout = options.Timeout_.GetOrElse(TConfig::Get()->TxTimeout);
    Init(auth, transactionId, actualTimeout);
}

TPingableTransaction::TPingableTransaction(
    const IClientRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TTransactionId& transactionId,
    ITransactionPingerPtr transactionPinger,
    const TAttachTransactionOptions& options)
    : ClientRetryPolicy_(retryPolicy)
    , Auth_(auth)
    , AbortableRegistry_(NDetail::TAbortableRegistry::Get())
    , AbortOnTermination_(options.AbortOnTermination_)
    , AutoPingable_(options.AutoPingable_)
    , Pinger_(std::move(transactionPinger))
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
    Init(auth, transactionId, timeout);
}

void TPingableTransaction::Init(
    const TAuth& auth,
    const TTransactionId& transactionId,
    TDuration timeout)
{
    TransactionId_ = transactionId;

    if (AbortOnTermination_) {
        AbortableRegistry_->Add(
            TransactionId_,
            ::MakeIntrusive<NDetail::TTransactionAbortable>(auth, TransactionId_));
    }

    if (AutoPingable_) {
        // Compute 'MaxPingInterval_' and 'MinPingInterval_' such that 'pingInterval == (max + min) / 2'.
        auto pingInterval = TConfig::Get()->PingInterval;
        auto safeTimeout = timeout - TDuration::Seconds(5);
        MaxPingInterval_ = Max(pingInterval, Min(safeTimeout, pingInterval * 1.5));
        MinPingInterval_ = pingInterval - (MaxPingInterval_ - pingInterval);

        Pinger_->RegisterTransaction(*this);
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

const std::pair<TDuration, TDuration> TPingableTransaction::GetPingInterval() const {
    return {MinPingInterval_, MaxPingInterval_};
}

const TAuth TPingableTransaction::GetAuth() const {
    return Auth_;
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
    if (Finalized_) {
        return;
    }

    Y_DEFER {
        Finalized_ = true;
        if (AutoPingable_ && Pinger_->HasTransaction(*this)) {
            Pinger_->RemoveTransaction(*this);
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
        ::TStringBuilder() << '#' << GetGuidAsString(lockId) << "/@node_id");
    return "#" + lockedNodeId.AsString();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
