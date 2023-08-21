#include "transaction_manager_detail.h"

namespace NYT::NTransactionSupervisor {

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
