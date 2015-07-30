#include "transaction.h"

#include "requests.h"

#include <mapreduce/yt/common/config.h>

#include <util/datetime/base.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TPingableTransaction::TPingableTransaction(
    const Stroka& serverName,
    const TTransactionId& parentId,
    const TMaybe<TDuration>& timeout,
    bool pingAncestors,
    const TMaybe<TNode>& attributes)
    : ServerName_(serverName)
    , Running_(false)
    , Thread_(Pinger, (void*)this)
{
    TransactionId_ = StartTransaction(
        serverName,
        parentId,
        timeout,
        pingAncestors,
        attributes);

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
    Running_ = false;
    Thread_.Join();
    if (commit) {
        CommitTransaction(ServerName_, TransactionId_);
    } else {
        AbortTransaction(ServerName_, TransactionId_);
    }
}

void TPingableTransaction::Pinger()
{
    while (Running_) {
        PingTransaction(ServerName_, TransactionId_);
        TInstant t = Now();
        while (Running_ && Now() - t < TConfig::Get()->PingInterval) {
            Sleep(TDuration::MilliSeconds(100));
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
