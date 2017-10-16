#include "transaction.h"


#include <mapreduce/yt/common/abortable_registry.h>
#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/finally_guard.h>
#include <mapreduce/yt/common/wait_proxy.h>
#include <mapreduce/yt/http/requests.h>

#include <util/datetime/base.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TPingableTransaction::TPingableTransaction(
    const TAuth& auth,
    const TTransactionId& parentId,
    const TMaybe<TDuration>& timeout,
    bool pingAncestors,
    const TMaybe<TString>& title,
    const TMaybe<TNode>& attributes)
    : Auth_(auth)
    , Thread_(TThread::TParams{Pinger, (void*)this}.SetName("pingable_tx"))
{
    TransactionId_ = StartTransaction(
        auth,
        parentId,
        timeout,
        pingAncestors,
        title,
        attributes);

    NDetail::TAbortableRegistry::Instance().Add(
        TransactionId_,
        ::MakeIntrusive<NDetail::TTransactionAbortable>(auth, TransactionId_));

    Running_ = true;
    Thread_.Start();
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
        Thread_.Join();
    });

    if (commit) {
        CommitTransaction(Auth_, TransactionId_);
    } else {
        AbortTransaction(Auth_, TransactionId_);
    }

    NDetail::TAbortableRegistry::Instance().Remove(TransactionId_);
}

void TPingableTransaction::Pinger()
{
    while (Running_) {
        PingTransaction(Auth_, TransactionId_);
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

} // namespace NYT
