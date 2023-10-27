#include "transaction_manager_detail.h"

#include <yt/yt/core/concurrency/fls.h>

namespace NYT::NTransactionSupervisor {

thread_local bool InTransactionAction;

bool IsInTransactionAction()
{
    return InTransactionAction;
}

TTransactionActionGuard::TTransactionActionGuard()
{
    YT_VERIFY(!InTransactionAction);
    InTransactionAction = true;
}

TTransactionActionGuard::~TTransactionActionGuard()
{
    YT_VERIFY(InTransactionAction);
    InTransactionAction = false;
}

////////////////////////////////////////////////////////////////////////////////

TTransactionIdPool::TTransactionIdPool(int maxSize)
    : MaxSize_(maxSize)
{ }

void TTransactionIdPool::Register(TTransactionId id)
{
    if (IdSet_.insert(id).second) {
        IdQueue_.push(id);
    }

    if (std::ssize(IdQueue_) > MaxSize_) {
        auto idToExpire = IdQueue_.front();
        IdQueue_.pop();
        YT_VERIFY(IdSet_.erase(idToExpire) == 1);
    }
}

bool TTransactionIdPool::IsRegistered(TTransactionId id) const
{
    return IdSet_.contains(id);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
