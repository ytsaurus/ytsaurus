#pragma once

#include "public.h"

#include "transaction_action.h"

#include <yt/yt/core/logging/logger_owner.h>

#include <yt/yt/core/misc/ring_queue.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

//! Returns |true| if the current thread is executing a transaction action.
bool IsInTransactionAction();

class TTransactionActionGuard
    : private TNonCopyable
{
public:
    TTransactionActionGuard();
    ~TTransactionActionGuard();
};

////////////////////////////////////////////////////////////////////////////////

template <class TTransaction>
class TTransactionManagerBase
    : public virtual NLogging::TLoggerOwner
{
public:
    void RegisterTransactionActionHandlers(TTransactionActionDescriptor<TTransaction> descriptor);

protected:
    THashMap<TString, TTransactionActionDescriptor<TTransaction>> ActionHandlerMap_;

    void RunPrepareTransactionActions(
        TTransaction* transaction,
        const TTransactionPrepareOptions& options,
        bool requireLegacyBehavior = false);
    void RunCommitTransactionActions(TTransaction* transaction, const TTransactionCommitOptions& options);
    void RunAbortTransactionActions(TTransaction* transaction, const TTransactionAbortOptions& options);
    void RunSerializeTransactionActions(TTransaction* transaction);
};

////////////////////////////////////////////////////////////////////////////////

//! Maintains a set of transaction ids of bounded capacity.
//! Expires old ids in FIFO order.
class TTransactionIdPool
{
public:
    explicit TTransactionIdPool(int maxSize);
    void Register(TTransactionId id);
    bool IsRegistered(TTransactionId id) const;

private:
    const int MaxSize_;
    THashSet<TTransactionId> IdSet_;
    TRingQueue<TTransactionId> IdQueue_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor

#define TRANSACTION_MANAGER_DETAIL_INL_H_
#include "transaction_manager_detail-inl.h"
#undef TRANSACTION_MANAGER_DETAIL_INL_H_
